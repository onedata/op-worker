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
                            shell_default:cd("../..")
                        end).

%% Returns absolute path to given file using virtual CWD which equals to current SUITE directory
-define(TEST_FILE(X), filename:join(ets:match(suite_state, {test_root, '$1'}) ++ [X])).

%% Returns absolute path to given file using virtual CWD which equals to ct_root/common_files
-define(COMMON_FILE(X), filename:join(ets:match(suite_state, {ct_root, '$1'}) ++ ["common_files"] ++ [X])).

%% ====================================================================
%% Assertion macros
%% ====================================================================

-undef(assert).
-define(assert(BoolExpr),
  ((fun () ->
    case (BoolExpr) of
      true -> ok;
      __V ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??BoolExpr)},
          {expected, true},
          {value, case __V of false -> __V;
                    _ -> {not_a_boolean,__V}
                  end}],
        ct:print("assertion_failed: ~p~n", [Args]),
        erlang:error({assertion_failed, Args})
    end
  end)())).

-undef(assertMatch).
-define(assertMatch(Guard, Expr),
  ((fun () ->
    case (Expr) of
      Guard -> ok;
      __V ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected, (??Guard)},
          {value, __V}],
        ct:print("assertMatch_failed: ~p~n", [Args]),
        erlang:error({assertMatch_failed, Args})
    end
  end)())).

-undef(assertEqual).
-define(assertEqual(Expect, Expr),
  ((fun (__X) ->
    case (Expr) of
      __X -> ok;
      __V ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected, __X},
          {value, __V}],
        ct:print("assertEqual_failed: ~p~n", [Args]),
        erlang:error({assertEqual_failed, Args})
    end
  end)(Expect))).

-undef(assertException).
-define(assertException(Class, Term, Expr),
  ((fun () ->
    try (Expr) of
      __V ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected,
            "{ "++(??Class)++" , "++(??Term)
              ++" , [...] }"},
          {unexpected_success, __V}],
        ct:print("assertException_failed: ~p~n", [Args]),
        erlang:error({assertException_failed, Args})
    catch
      Class:Term -> ok;
      __C:__T ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {expression, (??Expr)},
          {expected,
            "{ "++(??Class)++" , "++(??Term)
              ++" , [...] }"},
          {unexpected_exception,
            {__C, __T, erlang:get_stacktrace()}}],
        ct:print("assertException_failed: ~p~n", [Args]),
        erlang:error({assertException_failed, Args})
    end
  end)())).

-undef(cmdStatus).
-define(cmdStatus(N, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {(N), __Out} -> __Out;
      {__N, _} ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {command, (Cmd)},
          {expected_status,(N)},
          {status,__N}],
        ct:print("command_failed: ~p~n", [Args]),
        erlang:error({command_failed, Args})
    end
  end)())).

-undef(assertCmdStatus).
-define(assertCmdStatus(N, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {(N), _} -> ok;
      {__N, _} ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {command, (Cmd)},
          {expected_status,(N)},
          {status,__N}],
        ct:print("assertCmd_failed: ~p~n", [Args]),
        erlang:error({assertCmd_failed, Args})
    end
  end)())).

-undef(assertCmdOutput).
-define(assertCmdOutput(T, Cmd),
  ((fun () ->
    case ?_cmd_(Cmd) of
      {_, (T)} -> ok;
      {_, __T} ->
        Args = [{module, ?MODULE},
          {line, ?LINE},
          {command,(Cmd)},
          {expected_output,(T)},
          {output,__T}],
        ct:print("assertCmdOutput_failed: ~p~n", [Args]),
        erlang:error({assertCmdOutput_failed, Args})
    end
  end)())).