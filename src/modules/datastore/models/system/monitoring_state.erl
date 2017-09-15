%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of monitoring worker.
%%% @end
%%%-------------------------------------------------------------------
-module(monitoring_state).
-author("Michal Wrona").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([save/1, get/1, exists/1, update/2, delete/1, list/0]).
-export([run_in_critical_section/2, encode_id/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type key() :: #monitoring_id{} | datastore:key().
-type record() :: #monitoring_state{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(#monitoring_id{} = MonitoringId, Diff) ->
    monitoring_state:update(encode_id(MonitoringId), Diff);
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Returns monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(#monitoring_id{} = MonitoringId) ->
    monitoring_state:get(encode_id(MonitoringId));
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether monitoring state exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(#monitoring_id{} = MonitoringId) ->
    monitoring_state:exists(encode_id(MonitoringId));
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Deletes monitoring state.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(#monitoring_id{} = MonitoringId) ->
    monitoring_state:delete(encode_id(MonitoringId));
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure
%% that 2 funs with same ResourceId won't run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_in_critical_section(key(), Fun :: fun(() -> Result :: term())) ->
    Result :: term().
run_in_critical_section(#monitoring_id{} = MonitoringId, Fun) ->
    monitoring_state:run_in_critical_section(encode_id(MonitoringId), Fun);
run_in_critical_section(ResourceId, Fun) ->
    critical_section:run([?MODULE, ResourceId], Fun).

%%--------------------------------------------------------------------
%% @doc
%% Encodes monitoring_id record to datastore id.
%% @end
%%--------------------------------------------------------------------
-spec encode_id(#monitoring_id{}) -> datastore:key().
encode_id(MonitoringId) ->
    http_utils:base64url_encode(crypto:hash(md5, term_to_binary(MonitoringId))).

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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {monitoring_id, {record, [
            {main_subject_type, atom},
            {main_subject_id, binary},
            {metric_type, atom},
            {secondary_subject_type, atom},
            {secondary_subject_id, binary},
            {provider_id, binary}
        ]}},
        {rrd_path, string},
        {state_buffer, #{term => term}},
        {last_update_time, integer}
    ]}.