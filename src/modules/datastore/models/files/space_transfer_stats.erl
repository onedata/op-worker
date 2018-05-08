%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about transfers
%%% featuring given space and target provider.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfer_stats).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    key/2, key/3,
    get/1, get/2, get/3,
    update/4,
    delete/1, delete/2
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type timestamp() :: non_neg_integer().
-type size() :: pos_integer().
-type space_transfer_stats() :: #space_transfer_stats{}.
-type doc() :: datastore_doc:doc(space_transfer_stats()).

-export_type([space_transfer_stats/0, doc/0]).

% Space transfer stats docs store aggregated statistics of various transfers
% for the last 1 min, 1 hr, 1 day and 1 month. As such there is no conception
% of 'start_time' known from normal transfer docs. But for some functions from
% transfer_histograms module to work it is necessary to provide it.
% That's why a long past value like 0 (year 1970) is used.
-define(START_TIME, 0).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats id based on specified transfer type and
%% space (provider is assumed to be the calling one).
%% @end
%%-------------------------------------------------------------------
-spec key(TransferType :: binary(), SpaceId :: od_space:id()) -> binary().
key(TransferType, SpaceId) ->
    key(oneprovider:get_id(), TransferType, SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats id based on specified provider id,
%% transfer type and space id.
%% @end
%%-------------------------------------------------------------------
-spec key(ProviderId :: od_provider:id(), TransferType :: binary(),
    SpaceId :: od_space:id()) -> binary().
key(ProviderId, TransferType, SpaceId) ->
    RecordId = op_gui_utils:ids_to_association(TransferType, SpaceId),
    datastore_utils:gen_key(ProviderId, RecordId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for specified transfer stats id.
%% @end
%%-------------------------------------------------------------------
-spec get(TransferStatsId :: binary()) ->
    space_transfer_stats() | {error, term()}.
get(TransferStatsId) ->
    case datastore_model:get(?CTX, TransferStatsId) of
        {ok, Doc} -> {ok, Doc#document.value};
        Error -> Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for given transfer type, space and provider
%% calling this fun.
%% @end
%%-------------------------------------------------------------------
-spec get(TransferType :: binary(), SpaceId :: od_space:id()) ->
    space_transfer_stats() | {error, term()}.
get(TransferType, SpaceId) ->
    ?MODULE:get(key(TransferType, SpaceId)).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers stats for given transfer type, provider and space.
%% @end
%%-------------------------------------------------------------------
-spec get(ProviderId :: od_provider:id(), TransferType :: binary(),
    SpaceId :: od_space:id()) -> space_transfer_stats() | {error, term()}.
get(ProviderId, TransferType, SpaceId) ->
    ?MODULE:get(key(ProviderId, TransferType, SpaceId)).


%%--------------------------------------------------------------------
%% @doc
%% Updates space transfers stats document for given transfer type, space id
%% (provider is assumed to be the calling one) or creates it
%% if one doesn't exists already.
%% @end
%%--------------------------------------------------------------------
-spec update(TransferType :: binary(), SpaceId :: od_space:id(),
    BytesPerProvider :: #{od_provider:id() => size()}, CurrentTime :: timestamp()
) ->
    ok | {error, term()}.
update(TransferType, SpaceId, BytesPerProvider, CurrentTime) ->
    NewTimestamps = maps:map(fun(_, _) -> CurrentTime end, BytesPerProvider),
    Key = key(TransferType, SpaceId),
    Diff = fun(SpaceTransfers = #space_transfer_stats{
        last_update = LastUpdateMap,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        {ok, SpaceTransfers#space_transfer_stats{
            last_update = maps:merge(LastUpdateMap, NewTimestamps),
            min_hist = transfer_histograms:update(
                BytesPerProvider, MinHistograms, ?MINUTE_STAT_TYPE,
                LastUpdateMap, ?START_TIME, CurrentTime
            ),
            hr_hist = transfer_histograms:update(
                BytesPerProvider, HrHistograms, ?HOUR_STAT_TYPE,
                LastUpdateMap, ?START_TIME, CurrentTime
            ),
            dy_hist = transfer_histograms:update(
                BytesPerProvider, DyHistograms, ?DAY_STAT_TYPE,
                LastUpdateMap, ?START_TIME, CurrentTime
            ),
            mth_hist = transfer_histograms:update(
                BytesPerProvider, MthHistograms, ?MONTH_STAT_TYPE,
                LastUpdateMap, ?START_TIME, CurrentTime
            )
        }}
    end,
    Default = #document{
        scope = SpaceId,
        key = Key,
        value = #space_transfer_stats{
            last_update = NewTimestamps,
            min_hist = transfer_histograms:new(BytesPerProvider, ?MINUTE_STAT_TYPE),
            hr_hist = transfer_histograms:new(BytesPerProvider, ?HOUR_STAT_TYPE),
            dy_hist = transfer_histograms:new(BytesPerProvider, ?DAY_STAT_TYPE),
            mth_hist = transfer_histograms:new(BytesPerProvider, ?MONTH_STAT_TYPE)
        }
    },
    case datastore_model:update(?CTX, Key, Diff, Default) of
        {ok, _} -> ok;
        Error -> Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer stats document for specified transfer stats id.
%% @end
%%-------------------------------------------------------------------
-spec delete(TransferStatsId :: binary()) -> ok | {error, term()}.
delete(TransferStatsId) ->
    datastore_model:delete(?CTX, TransferStatsId).


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer stats document for given space and transfer type.
%% @end
%%-------------------------------------------------------------------
-spec delete(TransferType :: binary(), SpaceId :: od_space:id()) ->
    ok | {error, term()}.
delete(TransferType, SpaceId) ->
    delete(key(TransferType, SpaceId)).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
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
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]}.
