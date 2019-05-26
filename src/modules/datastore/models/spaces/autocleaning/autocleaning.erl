%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This models stores information considering auto-cleaning mechanism
%%% for given space.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: od_space:id().
-type record() :: #autocleaning{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore:doc(record()).
-type run_id() :: autocleaning_run:id().
-type config() :: autocleanig_config:config().
-type error() :: {error, term()}.

-export_type([id/0, run_id/0, record/0, doc/0, config/0]).

%% API
-export([get/1, is_enabled/1, get_config/1, get_current_run/1,
    create_or_update/2, maybe_mark_current_run/2, mark_run_finished/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-define(CTX, #{
    model => ?MODULE,
    generated_key => false
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(id()) -> {ok, doc()} | error().
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).

is_enabled(#autocleaning{config = Config}) ->
    autocleaning_config:is_enabled(Config).

-spec get_config(record() | doc() | id()) -> config() | undefined.
get_config(#autocleaning{config = Config}) ->
    Config;
get_config(#document{value = Autocleaning}) ->
    get_config(Autocleaning);
get_config(SpaceId) ->
    case autocleaning:get(SpaceId) of
        {ok, Doc} -> get_config(Doc);
        _ -> undefined
    end.

-spec get_current_run(record() | doc() | id()) -> run_id() | undefined.
get_current_run(#autocleaning{current_run = CurrentRun}) ->
    CurrentRun;
get_current_run(#document{value = AC}) ->
    get_current_run(AC);
get_current_run(SpaceId) ->
    case autocleaning:get(SpaceId) of
        {ok, ACDoc} ->
            get_current_run(ACDoc);
        {error, not_found} ->
            undefined
    end.

-spec create_or_update(id(), maps:map()) -> ok | error().
create_or_update(SpaceId, NewConfiguration) ->
    {ok, SupportSize} = provider_logic:get_support_size(SpaceId),
    case autocleaning:get(SpaceId) of
        {error, not_found} ->
            ?extract_ok(create(SpaceId, NewConfiguration, SupportSize));
        {ok, _} ->
            ?extract_ok(update(SpaceId, fun(AC = #autocleaning{config = CurrentConfig}) ->
                case autocleaning_config:create_or_update(CurrentConfig, NewConfiguration, SupportSize) of
                    {ok, NewConfig} ->
                        {ok, AC#autocleaning{config = NewConfig}};
                    Error ->
                        Error
                end
            end))
    end.

%%-------------------------------------------------------------------
%% @doc
%% Sets given AutocleaningRunId as current_run, if the field is
%% undefined. Otherwise does noting.
%% @end
%%-------------------------------------------------------------------
-spec maybe_mark_current_run(id(), run_id()) -> {ok, doc()} | error().
maybe_mark_current_run(SpaceId, ACRunId) ->
    update(SpaceId, fun
        (AC = #autocleaning{current_run = undefined}) ->
            {ok, AC#autocleaning{current_run = ACRunId}};
        (AC) ->
            {ok, AC}
    end).

-spec mark_run_finished(id()) -> ok.
mark_run_finished(SpaceId) ->
    ok = ?extract_ok(update(SpaceId, fun(AC) ->
        {ok, AC#autocleaning{current_run = undefined}}
    end)).

-spec delete(id()) -> ok.
delete(SpaceId) ->
    case get_current_run(SpaceId) of
        undefined -> ok;
        ARId ->
            autocleaning_controller:stop_cleaning(SpaceId),
            autocleaning_run:delete(ARId, SpaceId)
    end,
    datastore_model:delete(?CTX, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create(id(), maps:map(), non_neg_integer()) -> {ok, doc()} | error().
create(SpaceId, Configuration, SupportSize)->
    case default_doc(SpaceId, Configuration, SupportSize) of
        {ok, Doc} -> create(Doc);
        Error -> Error
    end.

-spec create(doc()) -> {ok, doc()}.
create(Doc) ->
    datastore_model:create(?CTX, Doc).

-spec update(id(), diff()) -> {ok, doc()} | error().
update(SpaceId, UpdateFun) ->
    datastore_model:update(?CTX, SpaceId, UpdateFun).

-spec default_doc(id(), maps:map(), non_neg_integer()) -> {ok, doc()} | error().
default_doc(SpaceId, Configuration, SupportSize) ->
    case autocleaning_config:create_or_update(undefined, Configuration, SupportSize) of
        {ok, NewConfig} ->
            {ok, #document{
                key = SpaceId,
                value = #autocleaning{
                    current_run = undefined,
                    config = NewConfig
                },
                scope = SpaceId
            }};
        Error -> Error
    end.

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
        {current_run, string},
        {config, {record, [
            {enabled, boolean},
            {target, integer},
            {threshold, integer},
            {rules, {record, [
                {enabled, boolean},
                {min_file_size, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {max_file_size, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {min_hours_since_last_open, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {max_open_count, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {max_hourly_moving_average, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {max_daily_moving_average, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}},
                {max_monthly_moving_average, {record, [
                    {enabled, boolean},
                    {value, integer}
                ]}}
            ]}}
        ]}}
    ]}.