%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This suite serves as regression tests for problems with space support.
%%% In rare cases, two providers supporting the same space could get a
%%% different information about space supports via GraphSync.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_space_support_test_SUITE).
-author("Lukasz Opiola").

-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    multiple_parallel_supports_test/1
]).

all() -> ?ALL([
    multiple_parallel_supports_test
]).


-define(USER_SELECTOR, space_owner).
-define(ATTEMPTS, 30).

-define(PARALLELISM, 10).
-define(REPEATS, 25).


%%%===================================================================
%%% Test functions
%%%===================================================================

multiple_parallel_supports_test(_Config) ->
    KrakowStorageId = panel_test_rpc:add_storage(krakow, #{?RAND_STR() => #{
        <<"type">> => <<"posix">>,
        <<"mountPoint">> => <<"/tmp">>
    }}),
    ParisStorageId = panel_test_rpc:add_storage(paris, #{?RAND_STR() => #{
        <<"type">> => <<"posix">>,
        <<"mountPoint">> => <<"/tmp">>
    }}),

    lists_utils:generate(fun(Ordinal) ->
        ct:pal("Repeat ~B", [Ordinal]),
        lists_utils:pforeach(fun(_) ->
            ?ct_catch_exceptions(begin
                timer:sleep(?RAND_INT(0, 2000)),
                SpaceId = create_supported_space(KrakowStorageId, ParisStorageId),
                try
                    utils:wait_until(fun() ->
                        KrakowRev = get_cached_space_revision(krakow, SpaceId),
                        is_integer(KrakowRev) andalso KrakowRev == get_cached_space_revision(paris, SpaceId)
                    end, 1000, ?ATTEMPTS)
                catch
                    error:timeout ->
                        KrakowCachedRecord = get_cached_space_record(krakow, SpaceId),
                        ParisCachedRecord = get_cached_space_record(paris, SpaceId),
                        ?ct_pal(?autoformat_with_msg("Cached revisions are not equal!", [
                            SpaceId,
                            KrakowCachedRecord,
                            ParisCachedRecord
                        ])),
                        throw(test_failed)
                end
            end)
        end, lists:seq(1, ?PARALLELISM))
    end, ?REPEATS).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
create_supported_space(KrakowStorageId, ParisStorageId) ->
    CreatorUserId = oct_background:get_user_id(?USER_SELECTOR),
    SpaceId = ozw_test_rpc:create_space(CreatorUserId, ?RAND_STR()),
    SupportToken = ozw_test_rpc:create_space_support_token(CreatorUserId, SpaceId),
    SpaceId = panel_test_rpc:support_space(krakow, KrakowStorageId, SupportToken, 1000000000),
    timer:sleep(?RAND_INT(200, 1000)),
    SpaceId = panel_test_rpc:support_space(paris, ParisStorageId, SupportToken, 1000000000).


%% @private
-spec get_cached_space_revision(oct_background:entity_selector(), od_space:id()) ->
    not_found | non_neg_integer().
get_cached_space_revision(ProviderSelector, SpaceId) ->
    case get_cached_space_record(ProviderSelector, SpaceId) of
        not_found ->
            not_found;
        #od_space{cache_state = CacheState} ->
            maps:get(revision, CacheState)
    end.


%% @private
-spec get_cached_space_record(oct_background:entity_selector(), od_space:id()) ->
    not_found | od_space:record().
get_cached_space_record(ProviderSelector, SpaceId) ->
    case opw_test_rpc:call(ProviderSelector, od_space, get_from_cache, [SpaceId]) of
        {error, not_found} ->
            % this can happen if the entry is temporarily invalidated, due to for example
            % problems with cache coalescing or lost Onezone connection - it will be
            % fetched and cached in short future, as the provider constantly requests space info
            not_found;
        {ok, #document{value = SpaceRecord}} ->
            SpaceRecord
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
