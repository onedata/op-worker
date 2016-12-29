%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C): 2013, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for special files.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_special).
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([mkdir/4, read_dir/4, get_child_attr/3]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Ctx :: fslogic_context:ctx(), ParentFile :: fslogic_worker:file(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?add_subcontainer, 2}, {?traverse_container, 2}]).
mkdir(Ctx, ParentFile, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},
    case file_meta:create(ParentFile, File) of
        {ok, DirUUID} ->
            {ok, _} = times:create(#document{key = DirUUID, value = #times{mtime = CTime, atime = CTime, ctime = CTime}}),
            fslogic_times:update_mtime_ctime(ParentFile, fslogic_context:get_user_id(Ctx)),
            #fuse_response{status = #status{code = ?OK}, fuse_response =
                #dir{uuid = fslogic_uuid:uuid_to_guid(DirUUID)}
            };
        {error, already_exists} ->
            #fuse_response{status = #status{code = ?EEXIST}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches attributes of directory's child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(Ctx :: fslogic_context:ctx(),
    ParentFile :: fslogic_worker:file(), Name :: file_meta:name()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?list_container, 2}]).
get_child_attr(Ctx, ParentFile, Name) ->
    UserId = fslogic_context:get_user_id(Ctx),
    UserRootUUID = fslogic_uuid:user_root_dir_uuid(UserId),
    {ok, ParentFileDoc} = file_meta:get(ParentFile),

    File = case ParentFileDoc#document.key of
        UserRootUUID ->
            {ok, #document{value = #od_user{space_aliases = Spaces}}} = od_user:get(UserId),
            case lists:keyfind(Name, 2, Spaces) of
                {SpaceId, _} -> {uuid, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)};
                false -> false
            end;

        _ ->
            case file_meta:resolve_path(ParentFileDoc, <<"/", Name/binary>>) of
                {ok, {F, _}} -> F;
                {error, {not_found, _}} -> false
            end
    end,

    case File of
        false -> #fuse_response{status = #status{code = ?ENOENT}};
        {uuid, Uuid} ->
            Guid = fslogic_uuid:uuid_to_guid(Uuid),
            FileInfo = file_info:new_by_guid(Guid),
            attr_req:get_file_attr(Ctx, FileInfo);
        #document{key = Uuid} ->
            Guid = fslogic_uuid:uuid_to_guid(Uuid),
            FileInfo = file_info:new_by_guid(Guid),
            attr_req:get_file_attr(Ctx, FileInfo)
    end.

%%--------------------------------------------------------------------
%% @doc Lists directory. Start with ROffset entity and limit returned list to RCount size.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_dir(Ctx :: fslogic_context:ctx(), File :: fslogic_worker:file(),
    Offset :: file_meta:offset(), Count :: file_meta:size()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?list_container, 2}]).
read_dir(Ctx, File, Offset, Size) ->
    SpaceId = fslogic_context:get_space_id(Ctx),
    ShareId = file_info:get_share_id(Ctx),
    UserId = fslogic_context:get_user_id(Ctx),
    {ok, #document{key = Key} = FileDoc} = file_meta:get(File),
    {ok, ChildLinks} = file_meta:list_children(FileDoc, Offset, Size),

    ?debug("read_dir ~p ~p ~p links: ~p", [File, Offset, Size, ChildLinks]),

    fslogic_times:update_atime(FileDoc, fslogic_context:get_user_id(Ctx)),

    UserRootUUID = fslogic_uuid:user_root_dir_uuid(UserId),
    case Key of
        UserRootUUID ->
            {ok, #document{value = #od_user{space_aliases = Spaces}}} =
                od_user:get(UserId),

            Children =
                case Offset < length(Spaces) of
                    true ->
                        SpacesChunk = lists:sublist(Spaces, Offset + 1, Size),
                        lists:map(fun({SpaceId, SpaceName}) ->
                            SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                            #child_link{uuid = fslogic_uuid:uuid_to_guid(SpaceUUID, SpaceId), name = SpaceName}
                        end, SpacesChunk);
                    false ->
                        []
                end,

            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_children{
                    child_links = Children
                }
            };
        _ ->
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_children{child_links = [CL#child_link{uuid = fslogic_uuid:uuid_to_share_guid(UUID, SpaceId, ShareId)}
                    || CL = #child_link{uuid = UUID} <- ChildLinks]}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
