%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Macros used in tests of storage import.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_OCT_TEST_HRL).

-define(SYNC_ACL, true).
-define(MAX_DEPTH, 9999999999999999999999).

-define(WORKERS(__CONFIG), [
    oct_background:get_random_provider_node(__CONFIG#storage_import_test_config.p1_selector),
    oct_background:get_random_provider_node(__CONFIG#storage_import_test_config.p2_selector)
]).

-define(ATTEMPTS, 30).
-define(SPACE_PATH, <<"/", (atom_to_binary(?FUNCTION_NAME))/binary>>).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId, Attempts),
    storage_import_oct_test_base:assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts)).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId),
    ?assertMonitoring(Worker, ExpectedSSM, SpaceId, 1)).

-record(storage_import_test_config, {
    p1_selector :: oct_background:entity_selector(),
    p2_selector :: oct_background:entity_selector(),
    space_selector = undefined :: undefined | oct_background:entity_selector(),
    import_config = undefined :: undefined | storage_import_oct_test_base:import_config()
}).

-record(import_config, {
    max_depth :: atom(),
    sync_acl :: integer()
}).

-endif.