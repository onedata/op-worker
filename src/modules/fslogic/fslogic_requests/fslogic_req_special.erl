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
-export([read_dir/4]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

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
