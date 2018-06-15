%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is used to handle changes of replica_eviction model.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_changes).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([handle/1]).

-define(run_if_is_self(ProviderId, F),
    case oneprovider:is_self(ProviderId) of
        true ->
            F(),
            ok;
        false ->
            ok
    end
).
%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Handles change of replica_eviction document.
%% @end
%%-------------------------------------------------------------------
-spec handle(replica_eviction:doc()) -> ok.
handle(REDoc = #document{value = #replica_eviction{
    action = request,
    requestee = Requestee
}}) ->
    ?run_if_is_self(Requestee, fun() ->
        handle_request(REDoc)
    end);
handle(REDoc = #document{value = #replica_eviction{
    action = confirm,
    requester = Requester
}}) ->
    ?run_if_is_self(Requester, fun() ->
        handle_confirmation(REDoc)
    end);
handle(REDoc = #document{value = #replica_eviction{
    action = refuse,
    requester = Requester
}}) ->
    ?run_if_is_self(Requester, fun() ->
        handle_refusal(REDoc)
    end);
handle(REDoc = #document{value = #replica_eviction{
    action = release_lock,
    requestee = Requestee
}}) ->
    ?run_if_is_self(Requestee, fun() ->
        handle_release_lock(REDoc)
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_eviction request.
%% @end
%%-------------------------------------------------------------------
-spec handle_request(replica_eviction:doc()) -> ok.
handle_request(#document{
    key = REId,
    value = RE = #replica_eviction{
        file_uuid = FileUuid
}}) ->
    case replica_eviction_lock:acquire_read_lock(FileUuid) of
        ok ->
            case can_support_eviction(RE) of
                {true, Blocks} ->
                    replica_eviction_communicator:confirm_invalidation_support(REId, Blocks);
                false ->
                    replica_eviction_lock:release_read_lock(FileUuid),
                    replica_eviction_communicator:refuse_invalidation_support(REId)
            end;
        error ->
            replica_eviction_communicator:refuse_invalidation_support(REId)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_eviction confirmation.
%% @end
%%-------------------------------------------------------------------
-spec handle_confirmation(replica_eviction:doc()) -> ok.
handle_confirmation(#document{
    key = REId,
    value = #replica_eviction{
        file_uuid = FileUuid,
        space_id = SpaceId,
        supported_blocks = Blocks,
        version_vector = VV,
        type = Type,
        report_id = ReportId
    }}) ->
    replica_evictor:notify_finished_task(SpaceId),
    replica_eviction_worker:cast(FileUuid, SpaceId, Blocks, VV, REId, Type, ReportId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_eviction refusal.
%% @end
%%-------------------------------------------------------------------
-spec handle_refusal(replica_eviction:doc()) -> ok.
handle_refusal(#document{
    key = REId,
    value = #replica_eviction{
        space_id = SpaceId,
        pid = Pid
}}) ->
    replica_evictor:notify_finished_task(SpaceId),
    %todo should we notify about error in different than sendin 0 bytes?
    replica_eviction_communicator:notify_failed_invalidation(Pid, REId, {error, invalidation_refused}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether local replica of file is sufficient to support eviction.
%% @end
%%-------------------------------------------------------------------
-spec can_support_eviction(replica_eviction:record()) -> boolean().
can_support_eviction(#replica_eviction{
    file_uuid = FileUuid,
    space_id = SpaceId,
    version_vector = VV,
    requested_blocks = RequestedBlocks
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {LocalLocationDoc, _FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    LocalBlocks = replica_finder:get_all_blocks([LocalLocationDoc]),
    case fslogic_blocks:invalidate(RequestedBlocks, LocalBlocks) of
        [] ->
            % todo currently works only if provider has all requested blocks
            LocalVV = file_location:get_version_vector(LocalLocationDoc),
            case version_vector:compare(LocalVV, VV)  of
                greater ->
                    {true, RequestedBlocks};
                identical ->
                    {true, RequestedBlocks};
                _ ->
                    false
            end;
        _ ->
            false
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_eviction release_lock action.
%% @end
%%-------------------------------------------------------------------
-spec handle_release_lock(replica_eviction:doc()) -> ok.
handle_release_lock(#document{
    key = REId,
    value = #replica_eviction{file_uuid = FileUuid}
}) ->
    replica_eviction:delete(REId),
    replica_eviction_lock:release_read_lock(FileUuid).