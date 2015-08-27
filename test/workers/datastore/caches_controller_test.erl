%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for caches_controller module.
%%% @end
%%%--------------------------------------------------------------------
-module(caches_controller_test).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include("global_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

mem_clearing_test_() ->
    {timeout, 60,
        fun() ->
            % utilize memory
            [{_, Mem0}] = monitoring:get_memory_stats(),
            FreeMem = 100 - Mem0,
            ToAdd = min(20, FreeMem/2),
            MemTarget = Mem0 + ToAdd/2,
            MemUsage = Mem0 + ToAdd,

            application:set_env(?APP_NAME, mem_to_clear_cache, MemTarget),

            OneMB = list_to_binary(prepare_list(1024*1024)),
            ets:new(test, [named_table, public, set]),

            Add100MB = fun(KeyBeg) ->
                for(1, 100, fun(I) ->
                    ets:insert(test, {1000*KeyBeg + I, binary:copy(OneMB)})
                end)
            end,
            Guard = fun() ->
                [{_, Current}] = monitoring:get_memory_stats(),
                Current >= MemUsage
            end,
            while(Add100MB, Guard),



            % clear memory
            meck:new(caches_controller, [passthrough]),
            meck:expect(caches_controller, delete_old_keys,
                fun
                    (locally_cached, TimeWindow) ->
                        ToDelete = timer:minutes(10),
                        case TimeWindow of
                            ToDelete ->  ets:delete(test);
                            _ -> ok
                        end;
                    (_, _) -> ok
                end
            ),

            ?assertEqual(mem_usage_too_high, caches_controller:clear_global_cache(MemUsage, false)),
            [{_, TmpMem}] = monitoring:get_memory_stats(),
            ?assert(TmpMem > MemTarget),

            ?assertEqual(mem_usage_too_high, caches_controller:clear_local_cache(MemUsage, false)),
            [{_, TmpMem2}] = monitoring:get_memory_stats(),
            ?assert(TmpMem2 > MemTarget),

            ?assertEqual(ok, caches_controller:clear_local_cache(MemUsage, true)),
            [{_, FinalMem}] = monitoring:get_memory_stats(),
            ?assert(FinalMem < MemTarget),

            ?assertEqual(ok, caches_controller:clear_global_cache(FinalMem, true)),

            catch ets:delete(test),



            % check
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [locally_cached, timer:hours(7*24)])),
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [locally_cached, timer:hours(24)])),
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [locally_cached, timer:hours(1)])),
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [locally_cached, timer:minutes(10)])),
            ?assertEqual(0, meck:num_calls(caches_controller, delete_old_keys, [locally_cached, 0])),

            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [globally_cached, timer:hours(7*24)])),
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [globally_cached, timer:hours(24)])),
            ?assertEqual(1, meck:num_calls(caches_controller, delete_old_keys, [globally_cached, timer:hours(1)])),
            ?assertEqual(0, meck:num_calls(caches_controller, delete_old_keys, [globally_cached, timer:minutes(10)])),
            ?assertEqual(0, meck:num_calls(caches_controller, delete_old_keys, [globally_cached, 0])),

            ?assert(meck:validate(caches_controller)),
            ok = meck:unload(caches_controller)
        end}.

get_hooks_config_test() ->
    Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
    XConfig = lists:map(fun(Method) ->
        {x, Method}
    end, Methods),
    YConfig = lists:map(fun(Method) ->
        {y, Method}
    end, Methods),
    ?assertEqual(YConfig ++ XConfig, caches_controller:get_hooks_config([x, y])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

prepare_list(1) ->
    "x";

prepare_list(Size) ->
    "x" ++ prepare_list(Size - 1).

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

while(F, Guard) ->
    while(0, F, Guard).

while(Counter, F, Guard) ->
    case Guard() of
        true ->
            ok;
        _ ->
            F(Counter),
            while(Counter+1, F, Guard)
    end.

-endif.