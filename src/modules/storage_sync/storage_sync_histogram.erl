%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for storing storage_sync monitoring data.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_histogram).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


-type key() :: exometer_report:metric().
-type histogram() :: #storage_sync_histogram{}.
-type doc() :: datastore_doc:doc(histogram()).
-type value() :: integer().
-type values() :: [value()].
-type timestamp() :: calendar:datetime().
-type length() :: non_neg_integer().

-export_type([key/0, value/0, values/0, timestamp/0, length/0]).


%% API
-export([new/1, add/2, get_histogram/1, remove/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined
}).

-define(RESOLUTION, application:get_env(?APP_NAME, storage_sync_histogram_length, 12)).
-define(KEY_SEPARATOR, <<"#">>).
-define(KEY_PREFIX, <<"sshist">>).

%%%===================================================================
%%% API functions
%%%===================================================================

new(Metric) ->
    Key = to_key(Metric),
    NewDoc = #document{
        key = Key,
        value = #storage_sync_histogram{
            values = [0 || _ <- lists:seq(1, ?RESOLUTION)],
            timestamp = calendar:local_time()
        }
    },
    {ok, _} = datastore_model:save(?CTX, NewDoc).

%%-------------------------------------------------------------------
%% @doc
%% Adds value to histogram associated with given metric.
%% @end
%%-------------------------------------------------------------------
-spec add(key(), value()) -> {ok , doc()}.
add(Metric, NewValue) ->
    {ok, _} = datastore_model:update(?CTX, to_key(Metric), fun(Old = #storage_sync_histogram{
        values = OldValues
    }) ->
        NewLength = length(OldValues) + 1,
        MaxLength = ?RESOLUTION,
        NewValues = lists:sublist(OldValues ++ [NewValue], NewLength - MaxLength + 1, MaxLength),
        {ok, Old#storage_sync_histogram{
            values = NewValues,
            timestamp = calendar:local_time()
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Returns saved values for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram(key()) -> {values(), timestamp()} | undefined.
get_histogram(Metric) ->
    case  datastore_model:get(?CTX, to_key(Metric)) of
        {ok, #document{value = #storage_sync_histogram{
            values = Values,
            timestamp = Timestamp
        }}}  ->
            {Values, Timestamp};
        _ ->
            undefined
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Removes entry for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec remove(key()) -> ok.
remove(Metric) ->
    ok = datastore_model:delete(?CTX, to_key(Metric)).


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

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec to_key(exometer_report:metric()) -> binary().
to_key(Metric) ->
    MetricBinary = [str_utils:to_binary(E) || E <- Metric],
    str_utils:join_binary([?KEY_PREFIX | MetricBinary], ?KEY_SEPARATOR).
