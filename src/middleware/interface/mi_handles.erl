%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing handles (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_handles).
-author("Katarzyna Such").

-include("middleware/middleware.hrl").

%% API
-export([
    create/5,
    get/3
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(
    session:id(), od_share:id(), od_handle_service:id(),
    od_handle:metadata_prefix(), od_handle:metadata()
) ->
    od_handle:id() | no_return().
create(SessionId, ShareId, HServiceId, MetadataPrefix, MetadataString) ->
    #document{value = #od_share{root_file = ShareGuid}} = ?check(share_logic:get(
        ?ROOT_SESS_ID, ShareId
    )),
    Guid = file_id:share_guid_to_guid(ShareGuid),

    middleware_worker:check_exec(SessionId, Guid, #handle_create_request{
        share_id = ShareId,
        handle_service_id = HServiceId,
        metadata_prefix =  MetadataPrefix,
        metadata_string = MetadataString
    }).


-spec get(session:id(), lfm:file_key(), od_handle:id()) -> od_handle:doc() | no_return().
get(SessionId, FileKey, HandleId) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #handle_get_request{handle_id = HandleId}).