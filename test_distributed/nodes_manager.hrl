-include_lib("common_test/include/ct.hrl").

-define(TEST, true).
-include_lib("eunit/include/eunit.hrl").

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
