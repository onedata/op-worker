%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of the view REST API - creation, retrieval,
%%% update and removal of view definitions, their listing and querying,
%%% along with the parameter sanitization and space privilege checks
%%% guarding all of them.
%%%
%%% A view is defined per space; its definition is dbsync-synced to every
%%% supporting provider, but evaluated (and hence queryable) only on the
%%% providers listed in the definition. All the test cases therefore
%%% exercise the API on more than one provider - the suite runs on three,
%%% which is the minimum for the view name to be ambiguous on a provider
%%% holding none of the conflicting definitions itself.
%%%
%%% The view evaluation semantics (what a map/reduce function emits for
%%% which document type) are covered by view_test_SUITE, which drives the
%%% internal API directly.
%%% @end
%%%-------------------------------------------------------------------
-module(view_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("view/view_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_get_update_delete_view/1,
    creating_view_with_invalid_params_should_fail/1,
    updating_view_with_invalid_params_should_fail/1,
    overwriting_view_should_fail/1,
    create_get_delete_reduce_fun/1,
    getting_nonexistent_view_should_fail/1,
    getting_view_of_space_unsupported_by_provider_should_fail/1,
    list_views/1,
    query_view/1,
    querying_view_with_invalid_params_should_fail/1,
    create_spatial_view/1,
    query_spatial_view/1,
    querying_spatial_view_with_wrong_function_should_fail/1,
    query_file_popularity_view/1,
    querying_spatial_view_requires_spatial_flag/1,
    removing_files_should_remove_them_from_view_results/1,
    create_duplicated_views_on_remote_providers/1
]).

all() -> [
    create_get_update_delete_view,
    creating_view_with_invalid_params_should_fail,
    updating_view_with_invalid_params_should_fail,
    overwriting_view_should_fail,
    create_get_delete_reduce_fun,
    getting_nonexistent_view_should_fail,
    getting_view_of_space_unsupported_by_provider_should_fail,
    list_views,
    query_view,
    querying_view_with_invalid_params_should_fail,
    create_spatial_view,
    query_spatial_view,
    querying_spatial_view_with_wrong_function_should_fail,
    query_file_popularity_view,
    querying_spatial_view_requires_spatial_flag,
    removing_files_should_remove_them_from_view_results,
    create_duplicated_views_on_remote_providers
].


-define(P1, krakow).
-define(P2, paris).
-define(P3, lisbon).
-define(ALL_PROVIDERS, [?P1, ?P2, ?P3]).

% space supported by all the providers - used by all the test cases but the one
% checking how a provider treats a space it does not support
-define(SPACE_SELECTOR, space1).
-define(SINGLE_PROVIDER_SPACE_SELECTOR, space_krk).

% a plain space member - unlike the space owner, who holds all the space
% privileges regardless of the ones assigned and could never be forbidden
-define(USER_SELECTOR, user2).

-define(ATTEMPTS, 60).

-define(VIEW_PATH(__SpaceId, __ViewName),
    <<"spaces/", __SpaceId/binary, "/views/", __ViewName/binary>>
).

-define(VIEW@(__ViewName, __ProviderIdPrefix),
    <<__ViewName/binary, "@", __ProviderIdPrefix/binary>>
).

% length of the provider id prefix appended to the names of ambiguously named
% views - see assert_provider_ids_are_told_apart_by_shortened_prefix/2
-define(SHORTENED_PROVIDER_ID_LEN, 4).

-define(JSON_METADATA_LOCATION_KEY(__CaseName),
    str_utils:format_bin("loc_~ts_~ts", [__CaseName, str_utils:rand_hex(6)])
).

% a failed REST call - either the code and body of an error response, or an error
% reported by the http client itself (see handle_json_response/1)
-type rest_failure() :: {non_neg_integer(), json_utils:json_term()} | {error, term()}.


%%%===================================================================
%%% Test functions
%%%===================================================================


create_get_update_delete_view(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P1Id = oct_background:get_provider_id(?P1),
    P2Id = oct_background:get_provider_id(?P2),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),
    ExpMapFunction = view_utils:escape_js_function(MapFunction),

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERR_FORBIDDEN),

    %% CREATE

    Options1 = #{<<"update_min_changes">> => 10000},

    % creating view without SPACE_MANAGE_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch([], list_views_via_rest(?P1, SpaceId, 100)),
    ?assertMatch(ErrorForbidden, create_view_via_rest(?P1, SpaceId, ViewName, MapFunction, Options1)),
    ?assertMatch([], list_views_via_rest(?P1, SpaceId, 100)),

    % creating view with SPACE_MANAGE_VIEWS privilege should succeed
    set_space_privileges([?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, create_view_via_rest(?P1, SpaceId, ViewName, MapFunction, Options1)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),

    %% GET

    % viewing view without SPACE_VIEW_VIEWS should fail
    set_space_privileges(AllPrivs -- [?SPACE_VIEW_VIEWS]),
    lists:foreach(fun(ProviderSelector) ->
        ?assertMatch(ErrorForbidden, get_view_via_rest(ProviderSelector, SpaceId, ViewName), ?ATTEMPTS)
    end, ?ALL_PROVIDERS),

    % viewing view with SPACE_VIEW_VIEWS should succeed
    set_space_privileges([?SPACE_VIEW_VIEWS]),
    assert_view_on_all_providers(SpaceId, ViewName, #{
        <<"viewOptions">> => Options1,
        <<"providers">> => [P1Id],
        <<"mapFunction">> => ExpMapFunction,
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    }),

    %% UPDATE

    Options2 = #{<<"replica_update_min_changes">> => 100},
    UpdateParams = Options2#{providers => [P2Id]},

    % updating view without SPACE_MANAGE_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, update_view_via_rest(?P1, SpaceId, ViewName, <<>>, UpdateParams)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),

    % assert nothing was changed
    assert_view_on_all_providers(SpaceId, ViewName, #{
        <<"viewOptions">> => Options1,
        <<"providers">> => [P1Id],
        <<"mapFunction">> => ExpMapFunction,
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    }),

    % updating view (without overriding map function) with SPACE_MANAGE_VIEWS privilege should succeed
    set_space_privileges([?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, update_view_via_rest(?P1, SpaceId, ViewName, <<>>, UpdateParams)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, #{
        <<"viewOptions">> => Options2,
        <<"providers">> => [P2Id],
        <<"mapFunction">> => ExpMapFunction,
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    }),

    % updating view (with overriding map function) with SPACE_MANAGE_VIEWS privilege should succeed
    OverridingMapFunction = view_test_utils:gen_reversed_map_function(
        file_test_utils:rand_xattr_name(?FUNCTION_NAME)
    ),
    ?assertMatch(ok, update_view_via_rest(?P1, SpaceId, ViewName, OverridingMapFunction, #{})),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, #{
        <<"viewOptions">> => Options2,
        <<"providers">> => [P2Id],
        <<"mapFunction">> => view_utils:escape_js_function(OverridingMapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    }),

    %% DELETE

    % deleting view without SPACE_MANAGE_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, remove_view_via_rest(?P2, SpaceId, ViewName)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),

    % deleting view with SPACE_MANAGE_VIEWS privilege should succeed
    set_space_privileges([?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, remove_view_via_rest(?P2, SpaceId, ViewName)),
    assert_views_listed_on_all_providers(SpaceId, []).


creating_view_with_invalid_params_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),

    lists:foreach(fun({Params, ExpError}) ->
        ProviderSelector = lists_utils:random_element(?ALL_PROVIDERS),
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ?assertMatch(ExpRestError, create_view_via_rest(
            ProviderSelector, SpaceId, ViewName, MapFunction, Params
        )),
        ?assertMatch([], list_views_via_rest(ProviderSelector, SpaceId, 100))
    end, invalid_view_definition_params(SpaceId)).


updating_view_with_invalid_params_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),

    InitialOptions = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_view_via_rest(?P1, SpaceId, ViewName, MapFunction, InitialOptions)),

    ExpView = #{
        <<"viewOptions">> => InitialOptions,
        <<"providers">> => [oct_background:get_provider_id(?P1)],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    assert_view_on_all_providers(SpaceId, ViewName, ExpView),

    lists:foreach(fun({Params, ExpError}) ->
        ProviderSelector = lists_utils:random_element(?ALL_PROVIDERS),
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ?assertEqual(ExpRestError, update_view_via_rest(
            ProviderSelector, SpaceId, ViewName, <<>>, Params
        )),
        ?assertEqual({ok, ExpView}, get_view_via_rest(ProviderSelector, SpaceId, ViewName))
    end, invalid_view_definition_params(SpaceId)).


overwriting_view_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P2Id = oct_background:get_provider_id(?P2),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),

    % create
    ?assertMatch([], list_views_via_rest(?P1, SpaceId, 100)),
    Options = #{
        <<"update_min_changes">> => 10000,
        <<"replica_update_min_changes">> => 100
    },
    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, MapFunction, Options#{providers => [P2Id]}
    )),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),

    ExpView = #{
        <<"viewOptions">> => Options,
        <<"providers">> => [P2Id],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    assert_view_on_all_providers(SpaceId, ViewName, ExpView),

    % overwrite
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_ALREADY_EXISTS),
    ?assertMatch(ExpRestError, create_view_via_rest(
        ?P1, SpaceId, ViewName,
        view_test_utils:gen_spatial_map_function(?JSON_METADATA_LOCATION_KEY(?FUNCTION_NAME)),
        #{spatial => true}
    )),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, ExpView),

    % delete
    ?assertMatch(ok, remove_view_via_rest(?P1, SpaceId, ViewName)),
    assert_views_listed_on_all_providers(SpaceId, []).


create_get_delete_reduce_fun(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P1Id = oct_background:get_provider_id(?P1),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),
    ReduceFunction = view_test_utils:gen_reduce_function(1),

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERR_FORBIDDEN),

    ExpViewWithReduceFun = fun(ExpReduceFunction) -> #{
        <<"viewOptions">> => #{},
        <<"providers">> => [P1Id],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => ExpReduceFunction,
        <<"spatial">> => false
    } end,

    % create on one provider
    ?assertMatch([], list_views_via_rest(?P1, SpaceId, 100)),
    ?assertMatch(ok, create_view_via_rest(?P1, SpaceId, ViewName, MapFunction, #{})),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, ExpViewWithReduceFun(null)),

    %% CREATE

    % adding view reduce fun without SPACE_MANAGE_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, add_reduce_fun_via_rest(?P2, SpaceId, ViewName, ReduceFunction)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, ExpViewWithReduceFun(null)),

    % adding view reduce fun with SPACE_MANAGE_VIEWS privilege should succeed
    set_space_privileges([?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, add_reduce_fun_via_rest(?P2, SpaceId, ViewName, ReduceFunction)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),

    ExpReduceFunction = view_utils:escape_js_function(ReduceFunction),
    assert_view_on_all_providers(SpaceId, ViewName, ExpViewWithReduceFun(ExpReduceFunction)),

    %% DELETE

    % deleting view reduce fun without SPACE_MANAGE_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, remove_reduce_fun_via_rest(?P1, SpaceId, ViewName)),
    assert_view_on_all_providers(SpaceId, ViewName, ExpViewWithReduceFun(ExpReduceFunction)),

    % deleting view reduce fun with SPACE_MANAGE_VIEWS privilege should succeed
    set_space_privileges([?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, remove_reduce_fun_via_rest(?P1, SpaceId, ViewName)),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, ExpViewWithReduceFun(null)),

    % delete the view itself
    ?assertMatch(ok, remove_view_via_rest(?P1, SpaceId, ViewName)),
    assert_views_listed_on_all_providers(SpaceId, []).


getting_nonexistent_view_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),

    lists:foreach(fun(ProviderSelector) ->
        ?assertMatch(ExpRestError, get_view_via_rest(ProviderSelector, SpaceId, ViewName))
    end, ?ALL_PROVIDERS).


getting_view_of_space_unsupported_by_provider_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SINGLE_PROVIDER_SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = gen_map_function(?FUNCTION_NAME),

    ?assertMatch(ok, create_view_via_rest(?P1, SpaceId, ViewName, MapFunction, #{})),
    ?assertEqual({ok, #{
        <<"viewOptions">> => #{},
        <<"providers">> => [oct_background:get_provider_id(?P1)],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    }}, get_view_via_rest(?P1, SpaceId, ViewName), ?ATTEMPTS),

    ForbiddenRestError = rest_test_utils:get_rest_error(?ERR_FORBIDDEN),
    lists:foreach(fun(ProviderSelector) ->
        % a provider not supporting the space is not authorized to fetch its record
        % from the zone, so it cannot tell whether the user holds the space privilege
        % required to view views - and refuses before ever checking the support ...
        ok = opw_test_rpc:call(ProviderSelector, gs_client_worker, invalidate_cache, [od_space, SpaceId]),
        ?assertMatch(ForbiddenRestError, get_view_via_rest(ProviderSelector, SpaceId, ViewName)),

        % ... unless the record has been fetched on behalf of a space member (e.g. when
        % listing their spaces) - it is cached then and the privilege check is made against
        % the cached copy, so the request is refused only for the lack of support
        ?assertMatch({ok, _}, opw_test_rpc:call(ProviderSelector, space_logic, get, [
            oct_background:get_user_session_id(?USER_SELECTOR, ProviderSelector), SpaceId
        ])),
        NotSupportedRestError = rest_test_utils:get_rest_error(?ERR_SPACE_NOT_SUPPORTED_BY(
            SpaceId, oct_background:get_provider_id(ProviderSelector)
        )),
        ?assertMatch(NotSupportedRestError, get_view_via_rest(ProviderSelector, SpaceId, ViewName))
    end, ?ALL_PROVIDERS -- [?P1]).


list_views(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewNum = 20,
    ChunkSize = rand:uniform(ViewNum),

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERR_FORBIDDEN),

    lists:foreach(fun(ProviderSelector) ->
        ?assertMatch([], list_views_via_rest(ProviderSelector, SpaceId, ChunkSize))
    end, ?ALL_PROVIDERS),

    ViewNames = lists:sort(lists:map(fun(Num) ->
        ProviderSelector = lists_utils:random_element(?ALL_PROVIDERS),
        ViewName = str_utils:format_bin("view_name_~2..0B", [Num]),
        ?assertMatch(ok, create_view_via_rest(
            ProviderSelector, SpaceId, ViewName, gen_map_function(?FUNCTION_NAME), #{}
        )),
        ViewName
    end, lists:seq(1, ViewNum))),

    lists:foreach(fun(ProviderSelector) ->
        ?assertEqual(ViewNames, list_views_via_rest(
            ProviderSelector, SpaceId, ChunkSize
        ), ?VIEW_SYNC_ATTEMPTS)
    end, ?ALL_PROVIDERS),

    % listing views without SPACE_VIEW_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_VIEW_VIEWS]),
    lists:foreach(fun(ProviderSelector) ->
        ?assertMatch(ErrorForbidden, list_views_via_rest(
            ProviderSelector, SpaceId, ChunkSize
        ), ?ATTEMPTS)
    end, ?ALL_PROVIDERS).


query_view(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P1Id = oct_background:get_provider_id(?P1),
    P2Id = oct_background:get_provider_id(?P2),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    XattrName = file_test_utils:rand_xattr_name(?FUNCTION_NAME),
    ExpGuids = create_files_with_xattr(?FUNCTION_NAME, 5, XattrName),

    AllPrivs = privileges:space_privileges(),
    ErrorNotFound = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERR_FORBIDDEN),

    % support view only by one provider; other should return error on query
    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, view_test_utils:gen_map_function(XattrName),
        #{providers => [P2Id]}
    )),
    ?assertMatch(ErrorNotFound, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    % support view on both providers and check that they return correct results
    ?assertMatch(ok, update_view_via_rest(
        ?P2, SpaceId, ViewName, <<>>, #{providers => [P1Id, P2Id]}
    ), ?ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    % remove support for view on one provider, which should then remove it from db
    ?assertMatch(ok, update_view_via_rest(
        ?P2, SpaceId, ViewName, <<>>, #{providers => [P2Id]}
    )),
    ?assertEqual(ErrorNotFound, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertMatch(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    % add view again on both providers and check that they return correct results
    ?assertMatch(ok, update_view_via_rest(
        ?P1, SpaceId, ViewName, <<>>, #{providers => [P1Id, P2Id]}
    )),
    ?assertEqual(ExpGuids, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    % querying view without SPACE_QUERY_VIEWS privilege should fail
    set_space_privileges(AllPrivs -- [?SPACE_QUERY_VIEWS]),
    ?assertEqual(ErrorForbidden, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?ATTEMPTS),
    ?assertEqual(ErrorForbidden, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?ATTEMPTS).


querying_view_with_invalid_params_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P1Id = oct_background:get_provider_id(?P1),
    P2Id = oct_background:get_provider_id(?P2),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    XattrName = file_test_utils:rand_xattr_name(?FUNCTION_NAME),
    ExpGuids = create_files_with_xattr(?FUNCTION_NAME, 5, XattrName),

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, view_test_utils:gen_map_function(XattrName),
        #{providers => [P1Id, P2Id]}
    )),
    ?assertEqual(ExpGuids, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    lists:foreach(fun({Params, ExpError}) ->
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ProviderSelector = lists_utils:random_element([?P1, ?P2]),
        ?assertMatch(ExpRestError, query_emitted_values(ProviderSelector, SpaceId, ViewName, Params))
    end, [
        {#{bbox => ok}, ?ERR_BAD_DATA(<<"bbox">>, undefined)},
        {#{bbox => 1}, ?ERR_BAD_DATA(<<"bbox">>, undefined)},

        {#{descending => ok}, ?ERR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {#{descending => 1}, ?ERR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {#{descending => -15.6}, ?ERR_BAD_VALUE_BOOLEAN(<<"descending">>)},

        {#{inclusive_end => ok}, ?ERR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},
        {#{inclusive_end => 1}, ?ERR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},
        {#{inclusive_end => -15.6}, ?ERR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},

        {#{keys => ok}, ?ERR_BAD_VALUE_JSON(<<"keys">>)},
        {#{keys => 1}, ?ERR_BAD_VALUE_JSON(<<"keys">>)},
        {#{keys => -15.6}, ?ERR_BAD_VALUE_JSON(<<"keys">>)},

        {#{limit => ok}, ?ERR_BAD_VALUE_INTEGER(<<"limit">>)},
        {#{limit => -3}, ?ERR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
        {#{limit => 15.2}, ?ERR_BAD_VALUE_INTEGER(<<"limit">>)},
        {#{limit => 0}, ?ERR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},

        {#{skip => ok}, ?ERR_BAD_VALUE_INTEGER(<<"skip">>)},
        {#{skip => -3}, ?ERR_BAD_VALUE_TOO_LOW(<<"skip">>, 1)},
        {#{skip => 15.2}, ?ERR_BAD_VALUE_INTEGER(<<"skip">>)},
        {#{skip => 0}, ?ERR_BAD_VALUE_TOO_LOW(<<"skip">>, 1)},

        {#{stale => da}, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => -3}, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => 15.2}, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => 0}, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},

        {#{spatial => 1}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => -3}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => ok}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)}
    ]).


create_spatial_view(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    MapFunction = view_test_utils:gen_spatial_map_function(
        ?JSON_METADATA_LOCATION_KEY(?FUNCTION_NAME)
    ),

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, MapFunction, #{spatial => true}
    )),
    assert_views_listed_on_all_providers(SpaceId, [ViewName]),
    assert_view_on_all_providers(SpaceId, ViewName, #{
        <<"viewOptions">> => #{},
        <<"providers">> => [oct_background:get_provider_id(?P1)],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => true
    }).


query_spatial_view(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    LocationKey = ?JSON_METADATA_LOCATION_KEY(?FUNCTION_NAME),

    % the file with no location set must not be emitted by the view
    [_Guid0, Guid1, Guid2, Guid3] = create_files(?FUNCTION_NAME, 4),
    set_file_location(Guid1, LocationKey, [5.1, 10.22]),
    set_file_location(Guid2, LocationKey, [0, 0]),
    set_file_location(Guid3, LocationKey, [10, 5]),

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, view_test_utils:gen_spatial_map_function(LocationKey),
        #{spatial => true, providers => all_provider_ids()}
    )),

    lists:foreach(fun(ProviderSelector) ->
        QueryParams = #{spatial => true, stale => false},
        ?assertEqual(
            lists:sort([Guid1, Guid2, Guid3]),
            query_emitted_values(ProviderSelector, SpaceId, ViewName, QueryParams),
            ?VIEW_SYNC_ATTEMPTS
        ),
        ?assertEqual(
            lists:sort([Guid1, Guid2]),
            query_emitted_values(ProviderSelector, SpaceId, ViewName, QueryParams#{
                start_range => <<"[0,0]">>,
                end_range => <<"[5.5,10.5]">>
            }),
            ?VIEW_SYNC_ATTEMPTS
        )
    end, ?ALL_PROVIDERS).


querying_spatial_view_with_wrong_function_should_fail(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    LocationKey = ?JSON_METADATA_LOCATION_KEY(?FUNCTION_NAME),

    [Guid] = create_files(?FUNCTION_NAME, 1),
    set_file_location(Guid, LocationKey, [5.1, 10.22]),

    % a spatial view must emit a geometry as the key - a string one makes the
    % query fail rather than return an empty result
    WrongSpatialMapFunction = <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['onedata_json'] && meta['onedata_json']['",
            LocationKey/binary, "']) {
            return [\"string\", id];
        }
        return null;
    }">>,

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, WrongSpatialMapFunction,
        #{spatial => true, providers => all_provider_ids()}
    )),

    lists:foreach(fun(ProviderSelector) ->
        ?assertMatch({?HTTP_400_BAD_REQUEST, _}, query_emitted_values(
            ProviderSelector, SpaceId, ViewName, #{spatial => true, stale => false}
        ), ?VIEW_SYNC_ATTEMPTS)
    end, ?ALL_PROVIDERS).


query_file_popularity_view(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ?assertMatch({ok, _}, query_view_via_rest(?P1, SpaceId, <<"file-popularity">>, #{
        spatial => false,
        stale => false
    })).


querying_spatial_view_requires_spatial_flag(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName,
        view_test_utils:gen_spatial_map_function(?JSON_METADATA_LOCATION_KEY(?FUNCTION_NAME)),
        #{spatial => true, providers => [oct_background:get_provider_id(?P1)]}
    )),

    ?assertMatch({?HTTP_404_NOT_FOUND, _}, query_view_via_rest(?P1, SpaceId, ViewName, #{})),
    ?assertMatch({ok, _}, query_view_via_rest(?P1, SpaceId, ViewName, #{spatial => true})).


removing_files_should_remove_them_from_view_results(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    XattrName = file_test_utils:rand_xattr_name(?FUNCTION_NAME),
    ExpGuids = create_files_with_xattr(?FUNCTION_NAME, 5, XattrName),

    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, view_test_utils:gen_map_function(XattrName),
        #{providers => [oct_background:get_provider_id(?P1), oct_background:get_provider_id(?P2)]}
    )),
    ?assertEqual(ExpGuids, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    remove_files(ExpGuids),

    ?assertEqual([], query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual([], query_emitted_values(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS).


create_duplicated_views_on_remote_providers(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    P1Id = oct_background:get_provider_id(?P1),
    P2Id = oct_background:get_provider_id(?P2),
    assert_provider_ids_are_told_apart_by_shortened_prefix(P1Id, P2Id),

    ViewName = view_test_utils:rand_view_name(?FUNCTION_NAME),
    XattrName = file_test_utils:rand_xattr_name(?FUNCTION_NAME),
    ExpGuids = create_files_with_xattr(?FUNCTION_NAME, 5, XattrName),

    MapFunction = view_test_utils:gen_map_function(XattrName),
    % the second view emits the same files but with the key and the value swapped,
    % which tells the two apart by their query results alone
    ReversedMapFunction = view_test_utils:gen_reversed_map_function(XattrName),

    Options = #{<<"update_min_changes">> => 10000},

    % create a view on P1 (evaluated by P1 and P2) and a different one, of the
    % same name, on P2 (evaluated by P2 only)
    ?assertMatch([], list_views_via_rest(?P1, SpaceId, 100)),
    ?assertMatch(ok, create_view_via_rest(
        ?P1, SpaceId, ViewName, MapFunction, Options#{providers => [P1Id, P2Id]}
    )),
    ?assertMatch(ok, create_view_via_rest(
        ?P2, SpaceId, ViewName, ReversedMapFunction, Options
    )),

    P1IdPrefix = shorten_provider_id(P1Id),
    P2IdPrefix = shorten_provider_id(P2Id),

    ViewName@P1 = ?VIEW@(ViewName, P1Id),
    ViewName@P2 = ?VIEW@(ViewName, P2Id),
    ViewName@P1Short = ?VIEW@(ViewName, P1IdPrefix),
    ViewName@P2Short = ?VIEW@(ViewName, P2IdPrefix),

    % a provider holding one of the conflicting definitions itself lists the remote
    % one first, disambiguated with the full provider id, and its own last under the
    % plain name; a provider holding neither tells them apart by a shortened prefix
    % and lists them in the order of their owners' ids - which, as the names differ
    % only in the appended prefix, is simply their sorted order
    ?assertMatch([ViewName@P2, ViewName], list_views_via_rest(?P1, SpaceId, 100), ?VIEW_SYNC_ATTEMPTS),
    ?assertMatch([ViewName@P1, ViewName], list_views_via_rest(?P2, SpaceId, 100), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(
        lists:sort([ViewName@P1Short, ViewName@P2Short]),
        list_views_via_rest(?P3, SpaceId, 100),
        ?VIEW_SYNC_ATTEMPTS
    ),

    ExpViewOfP1 = #{
        <<"viewOptions">> => Options,
        <<"providers">> => [P2Id, P1Id],
        <<"mapFunction">> => view_utils:escape_js_function(MapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    ExpViewOfP2 = #{
        <<"viewOptions">> => Options,
        <<"providers">> => [P2Id],
        <<"mapFunction">> => view_utils:escape_js_function(ReversedMapFunction),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    ErrorNotFound = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),
    ErrorAmbiguousId = rest_test_utils:get_rest_error(?ERR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>)),

    % get by simple name - resolves to the local definition, or fails as
    % ambiguous on the provider holding none
    ?assertEqual({ok, ExpViewOfP1}, get_view_via_rest(?P1, SpaceId, ViewName), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual({ok, ExpViewOfP2}, get_view_via_rest(?P2, SpaceId, ViewName), ?VIEW_SYNC_ATTEMPTS),
    ?assertMatch(ErrorAmbiguousId, get_view_via_rest(?P3, SpaceId, ViewName), ?VIEW_SYNC_ATTEMPTS),

    % get by extended name, both full and shortened
    lists:foreach(fun({ProviderSelector, ExtendedViewName, ExpResult}) ->
        ?assertEqual(ExpResult, get_view_via_rest(
            ProviderSelector, SpaceId, ExtendedViewName
        ), ?VIEW_SYNC_ATTEMPTS)
    end, [
        {?P1, ViewName@P1, {ok, ExpViewOfP1}},
        {?P2, ViewName@P2, {ok, ExpViewOfP2}},
        {?P3, ViewName@P1, {ok, ExpViewOfP1}},
        {?P3, ViewName@P2, {ok, ExpViewOfP2}},

        {?P1, ViewName@P1Short, {ok, ExpViewOfP1}},
        {?P2, ViewName@P2Short, {ok, ExpViewOfP2}},
        {?P3, ViewName@P1Short, {ok, ExpViewOfP1}},
        {?P3, ViewName@P2Short, {ok, ExpViewOfP2}}
    ]),

    % query by simple name - the emitted values identify which definition answered
    ?assertEqual(ExpGuids, query_emitted_values(?P1, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ExpGuids, query_emitted_keys(?P2, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),
    ?assertEqual(ErrorAmbiguousId, query_emitted_values(?P3, SpaceId, ViewName, #{}), ?VIEW_SYNC_ATTEMPTS),

    % query by extended name - only a provider evaluating the pointed definition
    % can answer, the rest report it as non existent
    lists:foreach(fun({ProviderSelector, QueryFun, ExtendedViewName, ExpResult}) ->
        ?assertEqual(ExpResult, QueryFun(
            ProviderSelector, SpaceId, ExtendedViewName, #{}
        ), ?VIEW_SYNC_ATTEMPTS)
    end, [
        {?P1, fun query_emitted_values/4, ViewName@P1, ExpGuids},
        {?P1, fun query_emitted_keys/4, ViewName@P2, ErrorNotFound},
        {?P2, fun query_emitted_values/4, ViewName@P1, ExpGuids},
        {?P2, fun query_emitted_keys/4, ViewName@P2, ExpGuids},
        {?P3, fun query_emitted_values/4, ViewName@P1, ErrorNotFound},
        {?P3, fun query_emitted_keys/4, ViewName@P2, ErrorNotFound},

        {?P1, fun query_emitted_values/4, ViewName@P1Short, ExpGuids},
        {?P1, fun query_emitted_keys/4, ViewName@P2Short, ErrorNotFound},
        {?P2, fun query_emitted_values/4, ViewName@P1Short, ExpGuids},
        {?P2, fun query_emitted_keys/4, ViewName@P2Short, ExpGuids},
        {?P3, fun query_emitted_values/4, ViewName@P1Short, ErrorNotFound},
        {?P3, fun query_emitted_keys/4, ViewName@P2Short, ErrorNotFound}
    ]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "3op",
        envs = [
            {op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {dbsync_changes_broadcast_interval, timer:seconds(1)}
            ]},
            {op_worker, cluster_worker, [
                {cache_to_disk_delay_ms, timer:seconds(1)},
                {cache_to_disk_force_delay_ms, timer:seconds(2)}
            ]}
        ],
        posthook = fun(NewConfig) ->
            % the space privileges of the test client are manipulated by the test
            % cases and restored before each of them (see init_per_testcase/2)
            set_space_privileges(privileges:space_privileges()),
            grant_all_privileges_in_single_provider_space(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 10}),
    NewConfig = lfm_proxy:init(Config),

    % a run interrupted before its teardown leaves behind whatever it created,
    % and the deployment is reused between runs - hence the cleanup here rather
    % than (only) in end_per_testcase
    set_space_privileges(privileges:space_privileges()),
    view_test_utils:remove_all_views(?ALL_PROVIDERS, oct_background:get_space_id(?SPACE_SELECTOR)),
    view_test_utils:remove_all_views(
        [?P1], oct_background:get_space_id(?SINGLE_PROVIDER_SPACE_SELECTOR)
    ),
    lfm_test_utils:clean_space(
        oct_background:get_random_provider_node(?P1),
        [oct_background:get_random_provider_node(P) || P <- ?ALL_PROVIDERS],
        oct_background:get_space_id(?SPACE_SELECTOR),
        ?ATTEMPTS
    ),

    case Case of
        query_file_popularity_view -> set_file_popularity_enabled(true);
        _ -> ok
    end,
    NewConfig.


end_per_testcase(Case, Config) ->
    case Case of
        query_file_popularity_view -> set_file_popularity_enabled(false);
        _ -> ok
    end,
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_space_privileges(privileges:privileges(privileges:space_privilege())) -> ok.
set_space_privileges(SpacePrivs) ->
    ozt_spaces:set_privileges(?SPACE_SELECTOR, ?USER_SELECTOR, SpacePrivs).


%% @private
%% The privileges in the single provider space are never manipulated by the test
%% cases, but must be granted once for its supporting provider to allow anything.
%% ozt_spaces:set_privileges/3 cannot be used here as it forces the space record
%% fetch on all the providers - which the ones not supporting it cannot do.
-spec grant_all_privileges_in_single_provider_space() -> ok.
grant_all_privileges_in_single_provider_space() ->
    SpaceId = oct_background:get_space_id(?SINGLE_PROVIDER_SPACE_SELECTOR),
    ozw_test_rpc:space_set_user_privileges(
        SpaceId, oct_background:get_user_id(?USER_SELECTOR), privileges:space_privileges()
    ),
    opt:force_fetch_entity(od_space, SpaceId, [?P1]).


%% @private
-spec set_file_popularity_enabled(boolean()) -> ok.
set_file_popularity_enabled(Enabled) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    Fun = case Enabled of
        true -> enable;
        false -> disable
    end,
    ok = opw_test_rpc:call(?P1, file_popularity_api, Fun, [SpaceId]).


%% @private
-spec all_provider_ids() -> [od_provider:id()].
all_provider_ids() ->
    [oct_background:get_provider_id(P) || P <- ?ALL_PROVIDERS].


%% @private
-spec gen_map_function(atom()) -> index:view_function().
gen_map_function(CaseName) ->
    view_test_utils:gen_map_function(file_test_utils:rand_xattr_name(CaseName)).


%% @private
-spec shorten_provider_id(od_provider:id()) -> binary().
shorten_provider_id(ProviderId) ->
    binary_part(ProviderId, 0, ?SHORTENED_PROVIDER_ID_LEN).


%% @private
%% Views of conflicting names are disambiguated with the shortest provider id
%% prefix that is unique among the conflicting definitions, at least
%% ?SHORTENED_PROVIDER_ID_LEN characters long. Provider ids are random, so a
%% longer common prefix is a (vanishingly unlikely) property of the deployment
%% rather than a defect - assert it explicitly instead of failing later on a
%% baffling name mismatch.
-spec assert_provider_ids_are_told_apart_by_shortened_prefix(
    od_provider:id(),
    od_provider:id()
) ->
    ok.
assert_provider_ids_are_told_apart_by_shortened_prefix(ProviderId1, ProviderId2) ->
    ?assert(binary:longest_common_prefix([ProviderId1, ProviderId2]) < ?SHORTENED_PROVIDER_ID_LEN),
    ok.


%% @private
-spec invalid_view_definition_params(od_space:id()) -> [{map(), errors:error()}].
invalid_view_definition_params(SpaceId) -> [
    {#{update_min_changes => ok}, ?ERR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
    {#{update_min_changes => -3}, ?ERR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},
    {#{update_min_changes => 15.2}, ?ERR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
    {#{update_min_changes => 0}, ?ERR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},

    {#{replica_update_min_changes => ok}, ?ERR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
    {#{replica_update_min_changes => -3}, ?ERR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},
    {#{replica_update_min_changes => 15.2}, ?ERR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
    {#{replica_update_min_changes => 0}, ?ERR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},

    {#{spatial => 1}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
    {#{spatial => -3}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
    {#{spatial => ok}, ?ERR_BAD_VALUE_BOOLEAN(<<"spatial">>)},

    {#{providers => [ok]}, ?ERR_SPACE_NOT_SUPPORTED_BY(SpaceId, <<"ok">>)},
    {#{providers => [<<"ASD">>]}, ?ERR_SPACE_NOT_SUPPORTED_BY(SpaceId, <<"ASD">>)}
].


%%%===================================================================
%%% Test file helpers
%%%===================================================================


%% @private
-spec create_files(atom(), pos_integer()) -> [file_id:file_guid()].
create_files(CaseName, FileCount) ->
    Node = oct_background:get_random_provider_node(?P1),
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?P1),
    SpaceDirGuid = space_dir:guid(oct_background:get_space_id(?SPACE_SELECTOR)),

    lists:map(fun(Num) ->
        FileName = str_utils:format_bin("~ts_~B", [CaseName, Num]),
        {ok, Guid} = lfm_proxy:create(Node, SessionId, SpaceDirGuid, FileName, undefined),
        Guid
    end, lists:seq(1, FileCount)).


%% @private
-spec create_files_with_xattr(atom(), pos_integer(), onedata_file:xattr_name()) ->
    [file_id:file_guid()].
create_files_with_xattr(CaseName, FileCount, XattrName) ->
    Node = oct_background:get_random_provider_node(?P1),
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?P1),

    Guids = create_files(CaseName, FileCount),
    lists:foreach(fun({Num, Guid}) ->
        ok = lfm_proxy:set_xattr(Node, SessionId, ?FILE_REF(Guid), #xattr{
            name = XattrName, value = Num
        })
    end, lists:enumerate(Guids)),

    lists:sort(Guids).


%% @private
-spec set_file_location(file_id:file_guid(), binary(), [number()]) -> ok.
set_file_location(Guid, JsonMetadataKey, Coordinates) ->
    ok = opt_file_metadata:set_custom_metadata(
        ?P1,
        oct_background:get_user_session_id(?USER_SELECTOR, ?P1),
        ?FILE_REF(Guid),
        json,
        #{<<"type">> => <<"Point">>, <<"coordinates">> => Coordinates},
        [JsonMetadataKey]
    ).


%% @private
-spec remove_files([file_id:file_guid()]) -> ok.
remove_files(Guids) ->
    Node = oct_background:get_random_provider_node(?P1),
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?P1),

    lists:foreach(fun(Guid) ->
        ok = lfm_proxy:unlink(Node, SessionId, ?FILE_REF(Guid))
    end, Guids).


%%%===================================================================
%%% REST helpers
%%%===================================================================


%% @private
-spec assert_view_on_all_providers(od_space:id(), index:name(), json_utils:json_map()) -> ok.
assert_view_on_all_providers(SpaceId, ViewName, ExpView) ->
    lists:foreach(fun(ProviderSelector) ->
        ?assertEqual({ok, ExpView}, get_view_via_rest(
            ProviderSelector, SpaceId, ViewName
        ), ?VIEW_SYNC_ATTEMPTS)
    end, ?ALL_PROVIDERS).


%% @private
-spec assert_views_listed_on_all_providers(od_space:id(), [index:name()]) -> ok.
assert_views_listed_on_all_providers(SpaceId, ExpViewNames) ->
    lists:foreach(fun(ProviderSelector) ->
        ?assertEqual(ExpViewNames, list_views_via_rest(
            ProviderSelector, SpaceId, 100
        ), ?VIEW_SYNC_ATTEMPTS)
    end, ?ALL_PROVIDERS).


%% @private
-spec create_view_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    index:view_function(),
    map()
) ->
    ok | rest_failure().
create_view_via_rest(ProviderSelector, SpaceId, ViewName, MapFunction, Params) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, (build_query_string(Params))/binary>>,
    handle_no_content_response(rest_test_utils:request(
        ProviderSelector, Path, put, js_content_auth_headers(), MapFunction
    )).


%% @private
-spec update_view_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    index:view_function(),
    map()
) ->
    ok | rest_failure().
update_view_via_rest(ProviderSelector, SpaceId, ViewName, MapFunction, Params) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, (build_query_string(Params))/binary>>,
    handle_no_content_response(rest_test_utils:request(
        ProviderSelector, Path, patch, js_content_auth_headers(), MapFunction
    )).


%% @private
-spec add_reduce_fun_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    index:view_function()
) ->
    ok | rest_failure().
add_reduce_fun_via_rest(ProviderSelector, SpaceId, ViewName, ReduceFunction) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, "/reduce">>,
    handle_no_content_response(rest_test_utils:request(
        ProviderSelector, Path, put, js_content_auth_headers(), ReduceFunction
    )).


%% @private
-spec remove_reduce_fun_via_rest(oct_background:entity_selector(), od_space:id(), index:name()) ->
    ok | rest_failure().
remove_reduce_fun_via_rest(ProviderSelector, SpaceId, ViewName) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, "/reduce">>,
    handle_no_content_response(rest_test_utils:request(
        ProviderSelector, Path, delete, auth_headers(), []
    )).


%% @private
-spec get_view_via_rest(oct_background:entity_selector(), od_space:id(), index:name()) ->
    {ok, json_utils:json_map()} | rest_failure().
get_view_via_rest(ProviderSelector, SpaceId, ViewName) ->
    handle_json_response(rest_test_utils:request(
        ProviderSelector, ?VIEW_PATH(SpaceId, ViewName), get,
        maps:put(?HDR_ACCEPT, <<"application/json">>, auth_headers()), []
    )).


%% @private
-spec remove_view_via_rest(oct_background:entity_selector(), od_space:id(), index:name()) ->
    ok | rest_failure().
remove_view_via_rest(ProviderSelector, SpaceId, ViewName) ->
    handle_no_content_response(rest_test_utils:request(
        ProviderSelector, ?VIEW_PATH(SpaceId, ViewName), delete, auth_headers(), []
    )).


%% @private
-spec query_view_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    map()
) ->
    {ok, json_utils:json_term()} | rest_failure().
query_view_via_rest(ProviderSelector, SpaceId, ViewName, Params) ->
    Path = <<
        (?VIEW_PATH(SpaceId, ViewName))/binary, "/query",
        (build_query_string(Params))/binary
    >>,
    handle_json_response(rest_test_utils:request(
        ProviderSelector, Path, get, auth_headers(), []
    )).


%% @private
%% Queries the view and returns the guids of the files identified by the emitted
%% values (see view_test_utils:gen_map_function/1).
-spec query_emitted_values(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    map()
) ->
    [file_id:file_guid()] | rest_failure().
query_emitted_values(ProviderSelector, SpaceId, ViewName, Params) ->
    query_emitted_object_ids(ProviderSelector, SpaceId, ViewName, Params, <<"value">>).


%% @private
%% Queries the view and returns the guids of the files identified by the emitted
%% keys (see view_test_utils:gen_reversed_map_function/1).
-spec query_emitted_keys(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    map()
) ->
    [file_id:file_guid()] | rest_failure().
query_emitted_keys(ProviderSelector, SpaceId, ViewName, Params) ->
    query_emitted_object_ids(ProviderSelector, SpaceId, ViewName, Params, <<"key">>).


%% @private
-spec query_emitted_object_ids(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    map(),
    binary()
) ->
    [file_id:file_guid()] | rest_failure().
query_emitted_object_ids(ProviderSelector, SpaceId, ViewName, Params, RowField) ->
    case query_view_via_rest(ProviderSelector, SpaceId, ViewName, Params) of
        {ok, Rows} when is_list(Rows) ->
            lists:sort(lists:map(fun(Row) ->
                {ok, Guid} = file_id:objectid_to_guid(maps:get(RowField, Row)),
                Guid
            end, Rows));
        Error ->
            Error
    end.


%% @private
-spec list_views_via_rest(oct_background:entity_selector(), od_space:id(), pos_integer()) ->
    [index:name()] | rest_failure().
list_views_via_rest(ProviderSelector, SpaceId, ChunkSize) ->
    case list_all_views_via_rest(ProviderSelector, SpaceId, ChunkSize, undefined, []) of
        Result when is_list(Result) ->
            % Make sure there are no duplicates
            ?assertEqual(lists:sort(Result), lists:usort(Result)),
            Result;
        Error ->
            Error
    end.


%% @private
-spec list_all_views_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    pos_integer(),
    undefined | binary(),
    [index:name()]
) ->
    [index:name()] | rest_failure().
list_all_views_via_rest(ProviderSelector, SpaceId, ChunkSize, PageToken, Acc) ->
    case list_views_chunk_via_rest(ProviderSelector, SpaceId, ChunkSize, PageToken) of
        {ok, {ViewNames, NextPageToken}} when NextPageToken =:= null ->
            Acc ++ ViewNames;
        {ok, {ViewNames, NextPageToken}} ->
            ?assertMatch(ChunkSize, length(ViewNames)),
            list_all_views_via_rest(
                ProviderSelector, SpaceId, ChunkSize, NextPageToken, Acc ++ ViewNames
            );
        Error ->
            Error
    end.


%% @private
-spec list_views_chunk_via_rest(
    oct_background:entity_selector(),
    od_space:id(),
    pos_integer(),
    undefined | binary()
) ->
    {ok, {[index:name()], null | binary()}} | rest_failure().
list_views_chunk_via_rest(ProviderSelector, SpaceId, ChunkSize, PageToken) ->
    Params = case PageToken of
        undefined -> #{limit => ChunkSize};
        _ -> #{limit => ChunkSize, page_token => PageToken}
    end,
    Path = <<"spaces/", SpaceId/binary, "/views", (build_query_string(Params))/binary>>,

    case handle_json_response(rest_test_utils:request(
        ProviderSelector, Path, get, auth_headers(), <<>>
    )) of
        {ok, #{<<"views">> := ViewNames} = Body} ->
            {ok, {ViewNames, maps:get(<<"nextPageToken">>, Body, null)}};
        Error ->
            Error
    end.


%% @private
-spec auth_headers() -> map().
auth_headers() ->
    #{?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(?USER_SELECTOR)}.


%% @private
-spec js_content_auth_headers() -> map().
js_content_auth_headers() ->
    maps:put(?HDR_CONTENT_TYPE, <<"application/javascript">>, auth_headers()).


%% @private
-spec handle_no_content_response(term()) -> ok | rest_failure().
handle_no_content_response({ok, ?HTTP_204_NO_CONTENT, _, _}) ->
    ok;
handle_no_content_response({ok, Code, _, Body}) ->
    {Code, json_utils:decode(Body)};
handle_no_content_response({error, _} = Error) ->
    Error.


%% @private
-spec handle_json_response(term()) ->
    {ok, json_utils:json_term()} | rest_failure().
handle_json_response({ok, ?HTTP_200_OK, _, Body}) ->
    {ok, json_utils:decode(Body)};
handle_json_response({ok, Code, _, Body}) ->
    {Code, json_utils:decode(Body)};
handle_json_response({error, _} = Error) ->
    % a query on a view whose index is still being built blocks until it is
    % ready, which can outlast the http client receive timeout - return the
    % error instead of crashing, so that the awaiting assertion retries it
    Error.


%% @private
-spec build_query_string(map()) -> binary().
build_query_string(Params) when map_size(Params) == 0 ->
    <<>>;
build_query_string(Params) ->
    lists:foldl(fun({Key, Value}, Acc) ->
        KeyBin = to_query_string_binary(Key),
        case Value of
            Values when is_list(Values) ->
                lists:foldl(fun(Val, Acc2) ->
                    <<Acc2/binary, "&", KeyBin/binary, "[]=", (to_query_string_binary(Val))/binary>>
                end, Acc, Values);
            _ ->
                <<Acc/binary, "&", KeyBin/binary, "=", (to_query_string_binary(Value))/binary>>
        end
    end, <<"?">>, maps:to_list(Params)).


%% @private
-spec to_query_string_binary(term()) -> binary().
to_query_string_binary(Val) when is_binary(Val) -> Val;
to_query_string_binary(Val) when is_integer(Val) -> integer_to_binary(Val);
to_query_string_binary(Val) when is_float(Val) -> float_to_binary(Val);
to_query_string_binary(Val) when is_atom(Val) -> atom_to_binary(Val, utf8).
