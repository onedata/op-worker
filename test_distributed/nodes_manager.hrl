-include_lib("common_test/include/ct.hrl").

-define(TEST, true).
-include_lib("eunit/include/eunit.hrl").

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
                            shell_default:cd("../.."),
                            os:cmd("./clear_test_db.sh"),
                            os:cmd("rm -rf /tmp/veilfs/*"),
                            os:cmd("rm -rf /tmp/veilfs2/*"),
                            os:cmd("rm -rf /tmp/veilfs3/*")
                        end).

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
-define(TEST_GROUP_EXTENDED, "veilfstestgroup(Grp)").
-define(TEST_GROUP2_EXTENDED, "veilfstestgroup2(Grp2)").

%% ====================================================================
%% Assertion macros
%% ====================================================================

-undef(assert).
-define(assert(BoolExpr),
  ((fun () ->
    case (BoolExpr) of
      true -> ok;
      __V ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??BoolExpr)},
          {expected, true},
          {value, case __V of false -> __V;
                    _ -> {not_a_boolean,__V}
                  end}],
        ct:print("assertion_failed: ~p~n", [__Args]),
        erlang:error({assertion_failed, __Args})
    end
  end)())).

-undef(assertMatch).
-define(assertMatch(Guard, Expr),
  ((fun () ->
    case (Expr) of
      Guard -> ok;
      __V ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected, (??Guard)},
          {value, __V}],
        ct:print("assertMatch_failed: ~p~n", [__Args]),
        erlang:error({assertMatch_failed, __Args})
    end
  end)())).

-undef(assertEqual).
-define(assertEqual(Expect, Expr),
  ((fun (__X) ->
    case (Expr) of
      __X -> ok;
      __V ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected, __X},
          {value, __V}],
        ct:print("assertEqual_failed: ~p~n", [__Args]),
        erlang:error({assertEqual_failed, __Args})
    end
  end)(Expect))).

-undef(assertException).
-define(assertException(Class, Term, Expr),
  ((fun () ->
    try (Expr) of
      __V ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected,
            "{ "++(??Class)++" , "++(??Term)
              ++" , [...] }"},
          {unexpected_success, __V}],
        ct:print("assertException_failed: ~p~n", [__Args]),
        erlang:error({assertException_failed, __Args})
    catch
      Class:Term -> ok;
      __C:__T ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected,
            "{ "++(??Class)++" , "++(??Term)
              ++" , [...] }"},
          {unexpected_exception,
            {__C, __T, erlang:get_stacktrace()}}],
        ct:print("assertException_failed: ~p~n", [__Args]),
        erlang:error({assertException_failed, __Args})
    end
  end)())).

-undef(cmdStatus).
-define(cmdStatus(N, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {(N), __Out} -> __Out;
      {__N, _} ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {command, (Cmd)},
          {expected_status,(N)},
          {status,__N}],
        ct:print("command_failed: ~p~n", [__Args]),
        erlang:error({command_failed, __Args})
    end
  end)())).

-undef(assertCmdStatus).
-define(assertCmdStatus(N, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {(N), _} -> ok;
      {__N, _} ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {command, (Cmd)},
          {expected_status,(N)},
          {status,__N}],
        ct:print("assertCmd_failed: ~p~n", [__Args]),
        erlang:error({assertCmd_failed, __Args})
    end
  end)())).

-undef(assertCmdOutput).
-define(assertCmdOutput(T, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {_, (T)} -> ok;
      {_, __T} ->
        __Args = [{module, ?MODULE},
          {line, ?LINE},
          {command,(Cmd)},
          {expected_output,(T)},
          {output,__T}],
        ct:print("assertCmdOutput_failed: ~p~n", [__Args]),
        erlang:error({assertCmdOutput_failed, __Args})
    end
  end)())).