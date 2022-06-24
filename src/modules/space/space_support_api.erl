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
-export([init_support_state/2, clean_support_state/1]).

-type support_opts() :: #{
    accounting_enabled := boolean(),
    dir_stats_enabled := boolean()
}.

-export_type([support_opts/0]).


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
            accounting_status = case AccountingEnabled of
                true -> enabled;
                false -> disabled
            end,
            dir_stats_collector_config = #dir_stats_collector_config{
                collecting_status = case DirStatsEnabled of
                    true -> enabled;
                    false -> disabled
                end
            }
        }
    }),
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
