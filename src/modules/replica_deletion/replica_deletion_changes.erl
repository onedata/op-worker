%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is used to handle changes of replica_deletion model.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_changes).
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
%% Handles change of replica_deletion document.
%% @end
%%-------------------------------------------------------------------
-spec handle(replica_deletion:doc()) -> ok.
handle(REDoc = #document{value = #replica_deletion{
    action = request,
    requestee = Requestee
}}) ->
    ?run_if_is_self(Requestee, fun() ->
        handle_request(REDoc)
    end);
handle(REDoc = #document{value = #replica_deletion{
    action = confirm,
    requester = Requester
}}) ->
    ?run_if_is_self(Requester, fun() ->
        handle_confirmation(REDoc)
    end);
handle(REDoc = #document{value = #replica_deletion{
    action = refuse,
    requester = Requester
}}) ->
    ?run_if_is_self(Requester, fun() ->
        handle_refusal(REDoc)
    end);
handle(REDoc = #document{value = #replica_deletion{
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
%% Handles replica_deletion request.
%% @end
%%-------------------------------------------------------------------
-spec handle_request(replica_deletion:doc()) -> ok.
handle_request(#document{
    key = RDId,
    value = RD = #replica_deletion{
        file_uuid = FileUuid
}}) ->
    case replica_deletion_lock:acquire_read_lock(FileUuid) of
        ok ->
            case can_support_deletion(RD) of
                {true, Blocks} ->
                    replica_deletion:confirm(RDId, Blocks);
                false ->
                    replica_deletion_lock:release_read_lock(FileUuid),
                    replica_deletion:refuse(RDId)
            end;
        _Error ->
            replica_deletion:refuse(RDId)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_deletion confirmation.
%% @end
%%-------------------------------------------------------------------
-spec handle_confirmation(replica_deletion:doc()) -> ok.
handle_confirmation(#document{
    key = RDId,
    value = #replica_deletion{
        file_uuid = FileUuid,
        space_id = SpaceId,
        supported_blocks = Blocks,
        version_vector = VV,
        type = Type,
        report_id = ReportId
    }}) ->
    replica_deletion_master:notify_finished_task(SpaceId),
    replica_deletion_worker:cast(FileUuid, SpaceId, Blocks, VV, RDId, Type, ReportId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles replica_deletion refusal.
%% @end
%%-------------------------------------------------------------------
-spec handle_refusal(replica_deletion:doc()) -> ok.
handle_refusal(#document{
    value = #replica_deletion{
        space_id = SpaceId,
        file_uuid = FileUuid,
        report_id = ReportId,
        type = Type
}}) ->
    replica_deletion_master:notify_finished_task(SpaceId),
    replica_deletion_master:process_result(Type, FileUuid, {error, invalidation_refused}, ReportId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether local replica of file is sufficient to support deletion.
%% @end
%%-------------------------------------------------------------------
-spec can_support_deletion(replica_deletion:record()) -> boolean().
can_support_deletion(#replica_deletion{
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
            % todo VFS-3728 currently works only if provider has all requested blocks
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
%% Handles replica_deletion release_lock action.
%% @end
%%-------------------------------------------------------------------
-spec handle_release_lock(replica_deletion:doc()) -> ok.
handle_release_lock(#document{
    key = RDId,
    value = #replica_deletion{file_uuid = FileUuid}
}) ->
    replica_deletion:delete(RDId),
    replica_deletion_lock:release_read_lock(FileUuid).