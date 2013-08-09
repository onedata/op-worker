%% This macro adds path to env_setter to code path so the test that uses env_setter should use this macro before first use of module
%% Also SUITE state is initialized
-define(INIT_DIST_TEST, begin
                            {ok, CWD} = file:get_cwd(),
                            ets:new(suite_state, [set, named_table, public]),
                            ets:delete_all_objects(suite_state),
                            ets:insert(suite_state, {test_root, filename:join(CWD, "..")}),
                            ets:insert(suite_state, {ct_root, filename:join(CWD, "../..")}),
                            code:add_path(filename:join(CWD, "../..")),
                            code:add_path(CWD),
                            shell_default:cd("../../..")
                        end).

%% Returns absolute path to given file using virtual CWD which equals to current SUITE directory
-define(TEST_FILE(X), filename:join(ets:match(suite_state, {test_root, '$1'}) ++ [X])).

%% Returns absolute path to given file using virtual CWD which equals to ct_root/common_files
-define(COMMON_FILE(X), filename:join(ets:match(suite_state, {ct_root, '$1'}) ++ ["common_files"] ++ [X])).