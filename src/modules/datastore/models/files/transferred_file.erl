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
-export([report_transfer_start/3, report_transfer_finish/3]).
-export([get_transfers/1]).
-export([clean_up/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).
-export([resolve_conflict/3]).

-type id() :: datastore:id().
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
-spec report_transfer_start(fslogic_worker:file_guid(), transfer:id(), transfer:timestamp()) ->
    ok | {error, term()}.
report_transfer_start(FileGuid, TransferId, ScheduleTime) ->
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),
    Entry = build_entry(TransferId, ScheduleTime),
    Diff = fun(Record) ->
        #transferred_file{
            ongoing_transfers = Ongoing,
            ended_transfers = Past
        } = Record,

        % Filter out entries for the same transfer but with other schedule time
        % and move them to past (the other entries are past runs of the transfer
        % that were unsuccessful).
        {Duplicates, OtherEntries} = find_all_duplicates(TransferId, Ongoing),

        {ok, Record#transferred_file{
            ongoing_transfers = ordsets:add_element(Entry, OtherEntries),
            ended_transfers = enforce_history_limit(ordsets:union(Duplicates, Past))
        }}
    end,
    Default = #document{key = file_guid_to_id(FileGuid),
        scope = SpaceId,
        value = #transferred_file{
            ongoing_transfers = ordsets:from_list([Entry])
        }
    },
    case datastore_model:update(?CTX, file_guid_to_id(FileGuid), Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Registers a finished transfer for given file/dir.
%% @end
%%--------------------------------------------------------------------
-spec report_transfer_finish(fslogic_worker:file_guid(), transfer:id(), transfer:timestamp()) ->
    ok | {error, term()}.
report_transfer_finish(FileGuid, TransferId, ScheduleTime) ->
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),
    Entry = build_entry(TransferId, ScheduleTime),
    Diff = fun(Record) ->
        #transferred_file{
            ongoing_transfers = OngoingTransfers,
            ended_transfers = PastTransfers
        } = Record,
        {ok, Record#transferred_file{
            ongoing_transfers = ordsets:del_element(Entry, OngoingTransfers),
            ended_transfers = enforce_history_limit(ordsets:add_element(Entry, PastTransfers))
        }}
    end,
    Default = #document{key = file_guid_to_id(FileGuid),
        scope = SpaceId,
        value = #transferred_file{
            ended_transfers = ordsets:from_list([Entry])
        }
    },
    case datastore_model:update(?CTX, file_guid_to_id(FileGuid), Diff, Default) of
        {ok, _} -> ok;
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
    MergedPastDoc = case datastore_utils:is_greater_rev(NewRev, PreviousRev) of
        true ->
            NewDoc#document{value = NewRecord#transferred_file{ended_transfers = AllPast}};
        false ->
            PrevDoc#document{value = PrevRecord#transferred_file{ended_transfers = AllPast}}
    end,

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
                    ongoing_transfers = AccOngoing,
                    ended_transfers = AccPast
                }} = AccDoc,
                case ordsets:is_element(Entry, AccPast) of
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
-spec file_guid_to_id(file_meta:guid()) -> id().
file_guid_to_id(FileGuid) ->
    datastore_utils:gen_key(<<>>, FileGuid).


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
    ordsets:from_list([entry_to_transfer_id(E) || E <- ordsets:to_list(Entries)]).


%% @private
-spec find_all_duplicates(transfer:id(), ordsets:ordset(entry())) ->
    {Duplicates :: ordsets:ordset(entry()), OtherEntries :: ordsets:ordset(entry())}.
find_all_duplicates(TransferId, Entries) ->
    ordsets:fold(fun(Entry, {DupAcc, OtherAcc}) ->
        case entry_to_transfer_id(Entry) of
            TransferId ->
                {ordsets:add_element(Entry, DupAcc), OtherAcc};
            _ ->
                {DupAcc, ordsets:add_element(Entry, OtherAcc)}
        end
    end, {ordsets:new(), ordsets:new()}, Entries).


%% @private
-spec enforce_history_limit([id()]) -> [id()].
enforce_history_limit(Transfers) ->
    Limit = ?HISTORY_LIMIT,
    case length(Transfers) > Limit of
        true -> lists:sublist(Transfers, Limit);
        false -> Transfers
    end.