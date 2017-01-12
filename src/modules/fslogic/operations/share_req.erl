%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests modifying shares.
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
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec create_share(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
create_share(Ctx, File, Name) ->
    Guid = file_ctx:get_guid_const(File),
    ShareId = datastore_utils:gen_uuid(),
    ShareGuid = fslogic_uuid:guid_to_share_guid(Guid, ShareId),

    Auth = user_ctx:get_auth(Ctx),
    SpaceId = file_ctx:get_space_id_const(File),
    {ok, _} = share_logic:create(Auth, ShareId, Name, SpaceId, ShareGuid),
    {ok, _} = file_meta:add_share(File, ShareId),
    #provider_response{status = #status{code = ?OK}, provider_response = #share{share_id = ShareId, share_file_uuid = ShareGuid}}.

%%--------------------------------------------------------------------
%% @doc
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
remove_share(Ctx, File) ->
    ShareId = file_ctx:get_share_id_const(File),
    Auth = user_ctx:get_auth(Ctx),
    ok = share_logic:delete(Auth, ShareId),

    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(File),
    {ok, _} = file_meta:remove_share(File, ShareId),
    ok = permissions_cache:invalidate_permissions_cache(file_meta, FileUuid), %todo pass file_ctx

    #provider_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
