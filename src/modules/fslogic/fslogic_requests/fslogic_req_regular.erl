%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for regular files.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_regular).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([get_parent/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Ctx :: fslogic_context:ctx(), File :: fslogic_worker:file()) ->
    ProviderResponse :: #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(Ctx, File) ->
    ShareId = file_info:get_share_id(Ctx),
    case ShareId of
        undefined ->
            SpacesBaseDirUUID = ?ROOT_DIR_UUID,
            {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
            case ParentUUID of
                SpacesBaseDirUUID ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_guid(fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(Ctx)), undefined)}
                    };
                _ ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid = fslogic_uuid:uuid_to_guid(ParentUUID)}
                    }
            end;
        _ ->
            SpaceId = fslogic_context:get_space_id(Ctx),
            {ok, #document{key = FileUuid, value = #file_meta{shares = Shares}}} = file_meta:get(File),
            case lists:member(ShareId, Shares) of
                true ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_share_guid(FileUuid, SpaceId, ShareId)}
                    };
                false ->
                    {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_share_guid(ParentUUID, SpaceId, ShareId)}
                    }
            end
    end.



%%%===================================================================
%%% Internal functions
%%%===================================================================