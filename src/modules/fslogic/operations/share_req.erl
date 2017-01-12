%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests modifying shares.
%%% @end
%%%--------------------------------------------------------------------
-module(share_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([create_share/3, remove_share/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Shares a given file
%% @end
%%--------------------------------------------------------------------
-spec create_share(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
create_share(UserCtx, FileCtx, Name) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    ShareId = datastore_utils:gen_uuid(),
    ShareGuid = fslogic_uuid:guid_to_share_guid(Guid, ShareId),

    Auth = user_ctx:get_auth(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, _} = share_logic:create(Auth, ShareId, Name, SpaceId, ShareGuid),
    {ok, _} = file_meta:add_share(FileCtx, ShareId),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #share{
            share_id = ShareId,
            share_file_uuid = ShareGuid
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Stops sharing a given file
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
remove_share(UserCtx, FileCtx) ->
    ShareId = file_ctx:get_share_id_const(FileCtx),
    Auth = user_ctx:get_auth(UserCtx),
    ok = share_logic:delete(Auth, ShareId),

    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    {ok, _} = file_meta:remove_share(FileCtx, ShareId),
    ok = permissions_cache:invalidate(file_meta, FileUuid), %todo pass file_ctx

    #provider_response{status = #status{code = ?OK}}.