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

-define(ATTEMPTS, 30).
-define(SPACE_PATH(), <<"/", (atom_to_binary(?FUNCTION_NAME))/binary>>).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId, Attempts),
    storage_import_oct_test_base:assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts)).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId),
    ?assertMonitoring(Worker, ExpectedSSM, SpaceId, 1)).

-record(import_config, {
    max_depth :: atom(),
    sync_acl :: integer()
}).

-type import_config() :: #import_config{}.

-record(storage_import_test_config, {
    space_id :: od_space:id(),
    imported_storage_id :: storage:id(),
    not_imported_storage_id :: storage:id(),
    import_config = #{} :: import_config()
}).

-endif.