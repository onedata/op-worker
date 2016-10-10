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
-export([mkdir/4, read_dir/4, link/3, read_link/2]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(CTX :: fslogic_worker:ctx(), ParentFile :: fslogic_worker:file(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?add_subcontainer, 2}, {?traverse_container, 2}]).
mkdir(CTX, ParentFile, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(CTX)
    }},
    case file_meta:create(ParentFile, File) of
        {ok, DirUUID} ->
            {ok, _} = times:create(#document{key = DirUUID, value = #times{mtime = CTime, atime = CTime, ctime = CTime}}),
            fslogic_times:update_mtime_ctime(ParentFile, fslogic_context:get_user_id(CTX)),
            #fuse_response{status = #status{code = ?OK}, fuse_response =
                #dir{uuid = fslogic_uuid:uuid_to_guid(DirUUID)}
            };
        {error, already_exists} ->
            #fuse_response{status = #status{code = ?EEXIST}}
    end.


%%--------------------------------------------------------------------
%% @doc Lists directory. Start with ROffset entity and limit returned list to RCount size.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_dir(CTX :: fslogic_worker:ctx(), File :: fslogic_worker:file(),
    Offset :: file_meta:offset(), Count :: file_meta:size()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?list_container, 2}]).
read_dir(#fslogic_ctx{space_id = SpaceId, share_id = ShareId} = CTX, File, Offset, Size) ->
    UserId = fslogic_context:get_user_id(CTX),
    {ok, #document{key = Key} = FileDoc} = file_meta:get(File),
    {ok, ChildLinks} = file_meta:list_children(FileDoc, Offset, Size),

    ?debug("read_dir ~p ~p ~p links: ~p", [File, Offset, Size, ChildLinks]),

    fslogic_times:update_atime(FileDoc, fslogic_context:get_user_id(CTX)),

    UserRootUUID = fslogic_uuid:user_root_dir_uuid(UserId),
    case Key of
        UserRootUUID ->
            {ok, #document{value = #onedata_user{spaces = Spaces}}} =
                onedata_user:get(UserId),

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
%% @doc Creates new symbolic link.
%% @end
%%--------------------------------------------------------------------
-spec link(fslogic_worker:ctx(), Path :: file_meta:path(), LinkValue :: binary()) ->
    no_return().
link(_CTX, _File, _LinkValue) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Gets value of symbolic link.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_link(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    no_return().
read_link(_CTX, _File) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
