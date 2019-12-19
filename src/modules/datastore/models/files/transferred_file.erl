%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model holding information if given file/directory is being transferred.
%%% @end
%%%-------------------------------------------------------------------
-module(transferred_file).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

% API
-export([
    report_transfer_start/3,
    report_transfer_finish/4,
    report_transfer_deletion/1
]).
-export([get_transfers/1]).
-export([clean_up/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).
-export([resolve_conflict/3]).

-type id() :: datastore:key().
-type record() :: #transferred_file{}.
-type doc() :: datastore_doc:doc(record()).

-type entry() :: binary(). % Concatenation of schedule time and transfer id

-export_type([id/0, doc/0, entry/0]).

% Inactivity time (in seconds) after which the history of past transfers will
% be erased.
-define(HISTORY_LIMIT, application:get_env(?APP_NAME, transfers_history_limit_per_file, 100)).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers a new transfer for given file/dir.
%% @end
%%--------------------------------------------------------------------
-spec report_transfer_start(fslogic_worker:file_guid(), transfer:id(),
    transfer:timestamp()) -> ok | {error, term()}.
report_transfer_start(FileGuid, TransferId, ScheduleTime) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    Entry = build_entry(TransferId, ScheduleTime),
    Key = file_guid_to_id(FileGuid),

    Diff = fun(Record) ->
        #transferred_file{ongoing_transfers = Ongoing} = Record,
        {ok, Record#transferred_file{
            ongoing_transfers = ordsets:add_element(Entry, Ongoing)
        }}
    end,
    Default = #document{
        key = Key,
        scope = SpaceId,
        value = #transferred_file{
            ongoing_transfers = ordsets:from_list([Entry])
        }
    },

    case datastore_model:update(?CTX, Key, Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Registers a finished transfer for given file/dir.
%% @end
%%--------------------------------------------------------------------
-spec report_transfer_finish(fslogic_worker:file_guid(), transfer:id(),
    transfer:timestamp(), transfer:timestamp()) -> ok | {error, term()}.
report_transfer_finish(FileGuid, TransferId, ScheduleTime, FinishTime) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    OldEntry = build_entry(TransferId, ScheduleTime),
    NewEntry = build_entry(TransferId, FinishTime),

    Key = file_guid_to_id(FileGuid),
    Diff = fun(Record) ->
        #transferred_file{
            ongoing_transfers = OngoingTransfers,
            ended_transfers = PastTransfers
        } = Record,

        {ok, Record#transferred_file{
            ongoing_transfers = ordsets:del_element(OldEntry, OngoingTransfers),
            ended_transfers = enforce_history_limit(ordsets:add_element(
                NewEntry, PastTransfers
            ))
        }}
    end,
    Default = #document{
        key = Key,
        scope = SpaceId,
        value = #transferred_file{
            ended_transfers = ordsets:from_list([NewEntry])
        }
    },

    case datastore_model:update(?CTX, Key, Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes given transfer (either active or finished) for given file.
%% @end
%%--------------------------------------------------------------------
-spec report_transfer_deletion(transfer:doc()) -> ok | {error, term()}.
report_transfer_deletion(#document{key = TransferId, value = Transfer}) ->
    #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        schedule_time = ScheduleTime,
        finish_time = FinishTime
    } = Transfer,

    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    OngoingEntry = build_entry(TransferId, ScheduleTime),
    PastEntry = build_entry(TransferId, FinishTime),

    Key = file_guid_to_id(FileGuid),
    Diff = fun(Record) ->
        #transferred_file{
            ongoing_transfers = OngoingTransfers,
            ended_transfers = PastTransfers
        } = Record,

        {ok, Record#transferred_file{
            ongoing_transfers = ordsets:del_element(
                OngoingEntry, OngoingTransfers
            ),
            ended_transfers = ordsets:del_element(PastEntry, PastTransfers)
        }}
    end,

    case datastore_model:update(?CTX, Key, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of ongoing transfers for given file/dir. The list includes
%% schedule times of the transfers.
%% @end
%%--------------------------------------------------------------------
-spec get_transfers(fslogic_worker:file_guid()) ->
    {ok, #{ongoing => ordsets:ordset(transfer:id()), ended => ordsets:ordset(transfer:id())}}.
get_transfers(FileGuid) ->
    case datastore_model:get(?CTX, file_guid_to_id(FileGuid)) of
        {ok, #document{value = TF}} ->
            {ok, #{
                ongoing => entries_to_transfer_ids(TF#transferred_file.ongoing_transfers),
                ended => entries_to_transfer_ids(TF#transferred_file.ended_transfers)
            }};
        {error, _} ->
            {ok, #{
                ongoing => [],
                ended => []
            }}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Cleans up all information about transfers for given FileGuid.
%% @end
%%--------------------------------------------------------------------
-spec clean_up(fslogic_worker:file_guid()) -> ok | {error, term()}.
clean_up(FileGuid) ->
    datastore_model:delete(?CTX, file_guid_to_id(FileGuid)).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {last_update, integer},
        {ongoing_tranfers, [string]},
        {past_tranfers, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {ongoing_tranfers, [string]},
        {ended_tranfers, [string]}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, _LastUpdate, Ongoing, Past}) ->
    ConvertEntries = fun(Entries) ->
        ordsets:from_list(lists:map(fun(Entry) ->
            [TransferId, TimestampBin] = binary:split(Entry, <<"|">>),
            build_entry(TransferId, binary_to_integer(TimestampBin))
        end, Entries))
    end,

    {2, #transferred_file{
        ongoing_transfers = ConvertEntries(Ongoing),
        ended_transfers = ConvertEntries(Past)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Provides custom resolution of remote, concurrent modification conflicts.
%% Should return 'default' if default conflict resolution should be applied.
%% Should return 'ignore' if new change is obsolete.
%% Should return '{Modified, Doc}' when custom conflict resolution has been
%% applied, where Modified defines whether next revision should be generated.
%% If Modified is set to 'false' conflict resolution outcome will be saved as
%% it is.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, NewDoc, PrevDoc) ->
    #document{revs = [NewRev | _], value = NewRecord = #transferred_file{
        ongoing_transfers = NewOngoing,
        ended_transfers = NewPast
    }} = NewDoc,
    #document{revs = [PreviousRev | _], value = PrevRecord = #transferred_file{
        ongoing_transfers = PrevOngoing,
        ended_transfers = PrevPast
    }} = PrevDoc,

    AllPast = enforce_history_limit(ordsets:union(NewPast, PrevPast)),
    MergedPastDoc = case datastore_rev:is_greater(NewRev, PreviousRev) of
        true ->
            NewDoc#document{value = NewRecord#transferred_file{ended_transfers = AllPast}};
        false ->
            PrevDoc#document{value = PrevRecord#transferred_file{ended_transfers = AllPast}}
    end,
    AllPastTransferIds = entries_to_transfer_ids(AllPast),

    % Disjunctive union of ongoing transfers to get those added to either one
    % of the docs
    AllAdded = ordsets:union(
        ordsets:subtract(NewOngoing, PrevOngoing),
        ordsets:subtract(PrevOngoing, NewOngoing)
    ),
    ResultDoc = case ordsets:size(AllAdded) of
        0 ->
            MergedPastDoc;
        _ ->
            % Drop any entries that were already marked as past
            ordsets:fold(fun(Entry, AccDoc) ->
                #document{value = Record = #transferred_file{
                    ongoing_transfers = AccOngoing
                }} = AccDoc,
                TransferId = entry_to_transfer_id(Entry),
                case lists:member(TransferId, AllPastTransferIds) of
                    true ->
                        AccDoc#document{value = Record#transferred_file{
                            ongoing_transfers = ordsets:del_element(Entry, AccOngoing)
                        }};
                    false ->
                        AccDoc#document{value = Record#transferred_file{
                            ongoing_transfers = ordsets:add_element(Entry, AccOngoing)
                        }}
                end
            end, MergedPastDoc, AllAdded)
    end,

    case ResultDoc of
        PrevDoc -> ignore;
        _ -> {true, ResultDoc}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec file_guid_to_id(fslogic_worker:file_guid()) -> id().
file_guid_to_id(FileGuid) ->
    file_id:guid_to_uuid(FileGuid).


%% @private
-spec build_entry(transfer:id(), transfer:timestamp()) -> entry().
build_entry(TransferId, Timestamp) ->
    {ok, LinkKey} = transfer:get_link_key(TransferId, Timestamp),
    <<LinkKey/binary, "|", TransferId/binary>>.


%% @private
-spec entry_to_transfer_id(entry()) -> transfer:id().
entry_to_transfer_id(Entry) ->
    [_LinkKey, TransferId] = binary:split(Entry, <<"|">>),
    TransferId.


%% @private
-spec entries_to_transfer_ids([entry()]) -> ordsets:ordset(transfer:id()).
entries_to_transfer_ids(Entries) ->
    [entry_to_transfer_id(E) || E <- ordsets:to_list(Entries)].


%% @private
-spec enforce_history_limit([id()]) -> [id()].
enforce_history_limit(Transfers) ->
    Limit = ?HISTORY_LIMIT,
    case length(Transfers) > Limit of
        true -> lists:sublist(Transfers, Limit);
        false -> Transfers
    end.