%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on space support state.
%%% @end
%%%-------------------------------------------------------------------
-module(space_support_api).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    init_support_state/2,
    get_support_opts/1,
    update_support_opts/2,
    clean_support_state/1
]).

-type support_opts() :: #{
    accounting_enabled := boolean(),
    dir_stats_enabled := boolean()
}.
-type support_opts_diff() :: #{
    accounting_enabled => boolean(),
    dir_stats_enabled => boolean()
}.

-export_type([support_opts/0, support_opts_diff/0]).


%% TODO rm
-define(DEFAULT_DIR_STATS_COLLECTING_STATUS(), op_worker:get_env(
    dir_stats_collecting_status_for_new_spaces, disabled
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_support_state(od_space:id(), support_opts()) -> ok.
init_support_state(SpaceId, SupportOpts = #{
    accounting_enabled := AccountingEnabled,
    dir_stats_enabled := DirStatsEnabled
}) ->
    assert_valid_support_opts(SupportOpts),

    {ok, _} = space_support_state:create(#document{
        key = SpaceId,
        value = #space_support_state{
            accounting_status = infer_status(AccountingEnabled),
            dir_stats_collector_config = #dir_stats_collector_config{
                collecting_status = infer_status(DirStatsEnabled)
            }
        }
    }),
    ok.


-spec get_support_opts(od_space:id()) -> {ok, support_opts()} | errors:error().
get_support_opts(SpaceId) ->
    case space_support_state:get(SpaceId) of
        {ok, #document{value = #space_support_state{
            accounting_status = AccountingStatus,
            dir_stats_collector_config = DirStatsCollectorConfig
        }}} ->
            {ok, #{
                accounting_enabled => case AccountingStatus of
                    enabled -> true;
                    disabled -> false
                end,
                dir_stats_enabled => dir_stats_collector_config:is_collecting_active(
                    DirStatsCollectorConfig
                )
            }};
        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.


-spec update_support_opts(od_space:id(), support_opts_diff()) -> ok | errors:error().
update_support_opts(SpaceId, SupportOptsDiff = #{accounting_enabled := AccountingEnabled}) ->
    assert_valid_support_opts(SupportOptsDiff),

    NewAccountingStatus = infer_status(AccountingEnabled),
    UpdateAccountingStatusDiff = fun
        (#space_support_state{accounting_status = Status}) when Status =:= NewAccountingStatus ->
            {error, no_change};
        (SpaceSupportState) ->
            {ok, SpaceSupportState#space_support_state{accounting_status = NewAccountingStatus}}
    end,

    case space_support_state:update(SpaceId, UpdateAccountingStatusDiff) of
        {ok, #document{value = #space_support_state{accounting_status = enabled}}} ->
            dir_stats_collector_config:enable(SpaceId);
        {ok, _} ->
            update_support_opts(SpaceId, maps:remove(accounting_enabled, SupportOptsDiff));
        {error, no_change} ->
            update_support_opts(SpaceId, maps:remove(accounting_enabled, SupportOptsDiff))
    end;

update_support_opts(SpaceId, #{dir_stats_enabled := true}) ->
    dir_stats_collector_config:enable(SpaceId);

update_support_opts(SpaceId, #{dir_stats_enabled := false}) ->
    dir_stats_collector_config:disable(SpaceId);

update_support_opts(_SpaceId, _SupportOptsDiff) ->
    ok.


-spec clean_support_state(od_space:id()) -> ok.
clean_support_state(SpaceId) ->
    ok = space_support_state:delete(SpaceId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_valid_support_opts(support_opts()) -> ok | no_return().
assert_valid_support_opts(#{
    accounting_enabled := true,
    dir_stats_enabled := false
}) ->
    throw(?ERROR_BAD_DATA(
        <<"dirStatsEnabled">>,
        <<"Collecting directory statistics can not be disabled when accounting is enabled.">>
    ));

assert_valid_support_opts(_) ->
    ok.


%% @private
-spec infer_status(boolean()) -> enabled | disabled.
infer_status(_Enabled = true) -> enabled;
infer_status(_Enabled = false) -> disabled.
