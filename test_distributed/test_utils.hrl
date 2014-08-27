%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains ct tests helper macros and defines.
%% @end
%% ===================================================================

-ifndef(TEST_UTILS_HRL).
-define(TEST_UTILS_HRL, 1).

-define(TEST, true).

-include("veil_modules/dao/dao_spaces.hrl").

-include("veil_modules/dao/dao_users.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Macro for cleaning test dirs before tests
-define(CLEAN_TEST_DIRS,begin
                            os:cmd("rm -rf /tmp/veilfs/*"),
                            os:cmd("rm -rf /tmp/veilfs2/*"),
                            os:cmd("rm -rf /tmp/veilfs3/*"),
                            os:cmd("./clear_test_db.sh")
                        end).

%% Veilcluster dependencies
-define(VEIL_DEPS, [sasl,lager,ssl,cowlib,ranch,cowboy,ibrowse]).

%% Returns absolute path to given file using virtual CWD which equals to current SUITE directory
-define(TEST_FILE(X), filename:join(ets:match(suite_state, {test_root, '$1'}) ++ [X])).

%% Returns absolute path to given file using virtual CWD which equals to ct_root/common_files
-define(COMMON_FILE(X), filename:join(ets:match(suite_state, {ct_root, '$1'}) ++ ["common_files"] ++ [X])).

%% Roots of test filesystem
-define(TEST_ROOT, "/tmp/veilfs/").
-define(TEST_ROOT2, "/tmp/veilfs2/").
-define(TEST_ROOT3, "/tmp/veilfs3/").

-define(ARG_TEST_ROOT, [?TEST_ROOT]).
-define(ARG_TEST_ROOT2, [?TEST_ROOT2]).
-define(ARG_TEST_ROOT3, [?TEST_ROOT3]).

% Test users and groups
-define(TEST_USER, "veilfstestuser").
-define(TEST_USER2, "veilfstestuser2").
-define(TEST_GROUP, "veilfstestgroup").
-define(TEST_GROUP2, "veilfstestgroup2").
-define(TEST_GROUP3, "veilfstestgroup3").
-define(TEST_GROUP_EXTENDED, "veilfstestgroup(Grp)").
-define(TEST_GROUP2_EXTENDED, "veilfstestgroup2(Grp2)").
-define(TEST_GROUP3_EXTENDED, "veilfstestgroup3(Grp3)").

-define(LOCAL_PROVIDER_ID, <<"providerId">>).
-define(ENABLE_PROVIDER(Config), ?ENABLE_PROVIDER(Config, ?LOCAL_PROVIDER_ID)).
-define(ENABLE_PROVIDER(Config, ProviderId),
    begin
        test_utils:ct_mock(Config, cluster_manager_lib, get_provider_id, fun() -> ProviderId end),
        Config
    end).

-endif.