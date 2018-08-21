%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for getting details and managing transfers.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_by_id).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([
    terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, delete_resource/2, content_types_accepted/2
]).

%% resource functions
-export([get_transfer/2, rerun_transfer/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) ->
    {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"DELETE">>, <<"POST">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) ->
    {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_transfer}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Cancels a scheduled or active transfer. Returns 400 in case
%% the transfer is already completed, canceled or failed.
%%
%% HTTP method: DELETE
%%
%% @param tid Transfer ID.
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    case transfer:cancel(Id) of
        ok ->
            {true, Req2, State2};
        {error, already_ended} ->
            throw(?ERROR_TRANSFER_ALREADY_ENDED)
    end.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', rerun_transfer}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}'
%% @doc Returns status of specific transfer. In case the transfer has
%% been scheduled for entire folder, the result is a list of transfer
%% statuses for each item in the folder.
%%
%% HTTP method: GET
%%
%% @param tid Transfer ID.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer(req(), maps:map()) -> {term(), req(), maps:map()}.
get_transfer(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),

    #{id := Id} = State2,

    {ok, #document{value = Transfer}} = transfer:get(Id),
    Response = json_utils:encode(transfer_to_json(Transfer)),
    {Response, Req2, State2}.

%%-------------------------------------------------------------------
%% '/api/v3/oneprovider/transfers/{tid}/rerun'
%% @doc Restarts transfer with given tid.
%%
%% HTTP method: POST
%%
%% @param tid Transfer ID.
%%-------------------------------------------------------------------
-spec rerun_transfer(req(), maps:map()) -> {term(), req(), maps:map()}.
rerun_transfer(Req, State) ->
    {State2, Req2} = validator:parse_id(Req, State),
    #{auth := SessionId, id := Tid} = State2,

    {ok, UserId} = session:get_user_id(SessionId),
    case transfer:rerun(UserId, Tid) of
        {ok, NewTransferId} ->
            Path = binary_to_list(<<"transfers/", NewTransferId/binary>>),
            Location = oneprovider:get_rest_endpoint(Path),
            Req3 = cowboy_req:reply(201, #{<<"location">> => Location}, Req2),
            {stop, Req3, State2};
        {error, not_ended} ->
            throw(?ERROR_TRANSFER_NOT_ENDED);
        {error, not_found} ->
            throw(?ERROR_TRANSFER_NOT_FOUND)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec transfer_to_json(Transfer :: transfer:transfer()) -> maps:map().
transfer_to_json(Transfer) ->
    #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        user_id = UserId,
        rerun_id = RerunId,
        path = Path,
        replication_status = ReplicationStatus,
        invalidation_status = InvalidationStatus,
        invalidating_provider = InvalidatingProvider,
        replicating_provider = ReplicatingProviderId,
        callback = Callback,
        files_to_process = FilesToProcess,
        files_processed = FilesProcessed,
        failed_files = FailedFiles,
        files_replicated = FilesReplicated,
        bytes_replicated = BytesReplicated,
        files_invalidated = FilesInvalidated,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        finish_time = FinishTime,
        last_update = LastUpdate,
        min_hist = MinHist,
        hr_hist = HrHist,
        dy_hist = DyHist,
        mth_hist = MthHist
    } = Transfer,
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),

    ReplicationStatusBin = atom_to_binary(ReplicationStatus, utf8),
    ReplicatingProvider = utils:ensure_defined(
        ReplicatingProviderId, undefined, null
    ),

    #{
        <<"fileId">> => FileObjectId,
        <<"userId">> => UserId,
        <<"rerunId">> => utils:ensure_defined(RerunId, undefined, null),
        <<"path">> => Path,
        <<"transferStatus">> => ReplicationStatusBin,
        <<"replicationStatus">> => ReplicationStatusBin,
        <<"invalidationStatus">> => atom_to_binary(InvalidationStatus, utf8),
        <<"targetProviderId">> => ReplicatingProvider,
        <<"replicatingProviderId">> => ReplicatingProvider,
        <<"invalidatingProviderId">> => utils:ensure_defined(
            InvalidatingProvider, undefined, null
        ),
        <<"callback">> => NullableCallback,
        <<"filesToProcess">> => FilesToProcess,
        <<"filesProcessed">> => FilesProcessed,
        <<"filesTransferred">> => FilesReplicated,
        <<"filesReplicated">> => FilesReplicated,
        <<"failedFiles">> => FailedFiles,
        <<"filesInvalidated">> => FilesInvalidated,
        <<"bytesTransferred">> => BytesReplicated,
        <<"bytesReplicated">> => BytesReplicated,
        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime,
        % It is possible that there is no last update, if 0 bytes were
        % transferred, in this case take the start time.
        <<"lastUpdate">> => lists:max([StartTime | maps:values(LastUpdate)]),
        <<"minHist">> => MinHist,
        <<"hrHist">> => HrHist,
        <<"dyHist">> => DyHist,
        <<"mthHist">> => MthHist
    }.
