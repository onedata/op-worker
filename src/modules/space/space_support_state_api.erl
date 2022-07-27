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
-module(space_support_state_api).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    init_support_state/2,
    get_support_state/1,
    update_support_opts/2,
    clean_support_state/1
]).
% Cluster upgrade API
-export([init_support_state_for_all_supported_spaces/0]).

%% TODO VFS-9587 replace opts with support parameters in oz
-type support_opts() :: #{
    accounting_enabled := boolean(),
    dir_stats_service_enabled := boolean()
}.
-type support_opts_diff() :: #{
    accounting_enabled => boolean(),
    dir_stats_service_enabled => boolean()
}.


%%%===================================================================
%%% API
%%%===================================================================


-spec init_support_state(od_space:id(), support_opts()) -> ok.
init_support_state(SpaceId, SupportOpts = #{
    accounting_enabled := AccountingEnabled,
    dir_stats_service_enabled := DirStatsServiceEnabled
}) ->
    assert_valid_support_opts(SupportOpts),

    {ok, _} = space_support_state:create(#document{
        key = SpaceId,
        value = #space_support_state{
            accounting_enabled = AccountingEnabled,
            dir_stats_service_state = #dir_stats_service_state{
                status = infer_status(DirStatsServiceEnabled)
            }
        }
    }),
    ok.


-spec get_support_state(od_space:id()) -> {ok, space_support_state:record()} | errors:error().
get_support_state(SpaceId) ->
    case space_support_state:get(SpaceId) of
        {ok, #document{value = SpaceSupportState}} ->
            {ok, SpaceSupportState};
        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.


-spec update_support_opts(od_space:id(), support_opts_diff()) -> ok | errors:error().
update_support_opts(SpaceId, SupportOptsDiff = #{accounting_enabled := NewAccountingEnabled}) ->
    assert_valid_support_opts(SupportOptsDiff),

    UpdateAccountingStatusDiff = fun
        (#space_support_state{accounting_enabled = Enabled}) when Enabled =:= NewAccountingEnabled ->
            {error, no_change};
        (SpaceSupportState) ->
            {ok, SpaceSupportState#space_support_state{accounting_enabled = NewAccountingEnabled}}
    end,

    case space_support_state:update(SpaceId, UpdateAccountingStatusDiff) of
        {ok, #document{value = #space_support_state{accounting_enabled = true}}} ->
            dir_stats_service_state:enable(SpaceId);
        {ok, _} ->
            update_support_opts(SpaceId, maps:remove(accounting_enabled, SupportOptsDiff));
        {error, no_change} ->
            update_support_opts(SpaceId, maps:remove(accounting_enabled, SupportOptsDiff))
    end;

update_support_opts(SpaceId, #{dir_stats_service_enabled := true}) ->
    dir_stats_service_state:enable(SpaceId);

update_support_opts(SpaceId, #{dir_stats_service_enabled := false}) ->
    dir_stats_service_state:disable(SpaceId);

update_support_opts(_SpaceId, _SupportOptsDiff) ->
    ok.


-spec clean_support_state(od_space:id()) -> ok.
clean_support_state(SpaceId) ->
    ok = space_support_state:delete(SpaceId).


%%%===================================================================
%%% Cluster upgrade API
%%%===================================================================


-spec init_support_state_for_all_supported_spaces() -> ok.
init_support_state_for_all_supported_spaces() ->
    {ok, SupportedSpaceIds} = provider_logic:get_spaces(),

    lists:foreach(fun(SpaceId) ->
        space_support_state:delete(SpaceId),

        init_support_state(SpaceId, #{
            accounting_enabled => false,
            dir_stats_service_enabled => false
        })
    end, SupportedSpaceIds).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_valid_support_opts(support_opts()) -> ok | no_return().
assert_valid_support_opts(#{
    accounting_enabled := true,
    dir_stats_service_enabled := false
}) ->
    throw(?ERROR_BAD_DATA(
        <<"dirStatsServiceEnabled">>,
        <<"Collecting directory statistics can not be disabled when accounting is enabled.">>
    ));

assert_valid_support_opts(_) ->
    ok.


%% @private
-spec infer_status(boolean()) -> enabled | disabled.
infer_status(_Enabled = true) -> enabled;
infer_status(_Enabled = false) -> disabled.
