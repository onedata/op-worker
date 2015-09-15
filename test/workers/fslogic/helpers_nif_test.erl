%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test for basic context management in helpers_nif module
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif_test).
-author("Rafal Slota").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

new_ctx_test() ->
    ok = helpers_nif:init(),
    ?assertMatch({ok, _}, helpers_nif:new_helper_ctx()),
    ok.

new_obj_test() ->
    ok = helpers_nif:init(),
    ?assertMatch({ok, _}, helpers_nif:new_helper_obj(<<"DirectIO">>, [<<"/tmp">>])),
    ok.

ctx_test_() ->
    {setup,
        fun() ->
            ok = helpers_nif:init(),
            {ok, CTX} = helpers_nif:new_helper_ctx(),
            CTX
        end,
        fun(CTX) ->
            [
                {"File descriptor is set correctly",
                    fun() ->
                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 15)),
                        ?assertMatch({ok, 15}, helpers_nif:get_fd(CTX)),

                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 0)),
                        ?assertMatch({ok, 0}, helpers_nif:get_fd(CTX)),

                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 5)),
                        ?assertMatch({ok, 5}, helpers_nif:get_fd(CTX)),
                        ok
                    end},
                {"Flags are set correctly",
                    fun() ->
                        ?assertMatch(ok, helpers_nif:set_flags(CTX, [])),
                        ?assertMatch({ok, ['O_RDONLY']}, helpers_nif:get_flags(CTX)),

                        ?assertMatch(ok, helpers_nif:set_flags(CTX, ['O_RDWR'])),
                        ?assertMatch({ok, ['O_RDWR']}, helpers_nif:get_flags(CTX)),

                        ?assertMatch(ok, helpers_nif:set_flags(CTX, ['O_RDONLY', 'O_NONBLOCK'])),
                        Res0 = helpers_nif:get_flags(CTX),
                        ?assertMatch({ok, [_, _]}, Res0),
                        {ok, Flags0} = Res0,
                        ?assert(lists:member('O_RDONLY', Flags0)),
                        ?assert(lists:member('O_NONBLOCK', Flags0)),

                        ?assertMatch(ok, helpers_nif:set_flags(CTX, ['O_WRONLY', 'O_NONBLOCK', 'O_ASYNC', 'O_TRUNC'])),
                        Res1 = helpers_nif:get_flags(CTX),
                        ?assertMatch({ok, [_, _, _, _]}, Res1),
                        {ok, Flags1} = Res1,
                        ?assert(lists:member('O_WRONLY', Flags1)),
                        ?assert(lists:member('O_NONBLOCK', Flags1)),
                        ?assert(lists:member('O_ASYNC', Flags1)),
                        ?assert(lists:member('O_TRUNC', Flags1)),

                        ?assertError(badarg, helpers_nif:set_flags(CTX, ['O_RDONLY', 'O_NONBLOCK', "unknown_type"])),
                        ?assertError(badarg, helpers_nif:set_flags(CTX, ['O_RDONLY', 'O_NONBLOCK', 'unknown_flag'])),

                        ok
                    end},
                {"User is set correctly",
                    fun() ->
                        ?assertMatch(ok, helpers_nif:set_user_ctx(CTX, 0, 0)),
                        ?assertMatch({ok, {0, 0}}, helpers_nif:get_user_ctx(CTX)),

                        ?assertMatch(ok, helpers_nif:set_user_ctx(CTX, -1, -1)),
                        ?assertNotMatch({ok, {-1, -1}}, helpers_nif:get_user_ctx(CTX)),

                        ?assertMatch(ok, helpers_nif:set_user_ctx(CTX, 1001, 1002)),
                        ?assertMatch({ok, {1001, 1002}}, helpers_nif:get_user_ctx(CTX)),

                        ?assertMatch(ok, helpers_nif:set_user_ctx(CTX, 432423, 8953275)),
                        ?assertMatch({ok, {432423, 8953275}}, helpers_nif:get_user_ctx(CTX)),

                        ok
                    end},
                {"File handle is set correctly",
                    fun() ->
                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 5)),
                        ?assertMatch({ok, 5}, helpers_nif:get_fd(CTX)),

                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 104)),
                        ?assertMatch({ok, 104}, helpers_nif:get_fd(CTX)),

                        ?assertMatch(ok, helpers_nif:set_fd(CTX, 0)),
                        ?assertMatch({ok, 0}, helpers_nif:get_fd(CTX)),

                        ok
                    end}
            ]
        end
    }.

username_to_uid_test() ->
    ok = helpers_nif:init(),
    ?assertMatch({ok, 0}, helpers_nif:username_to_uid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:username_to_uid(<<"sadmlknfqlwknd">>)),
    ok.

groupname_to_gid_test() ->
    ok = helpers_nif:init(),
    ?assertMatch({ok, 0}, helpers_nif:groupname_to_gid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:groupname_to_gid(<<"sadmlknfqlwknd">>)),
    ok.

-endif.