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

-include("global_definitions.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

helpers_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun new_obj/0,
            fun username_to_uid/0,
            fun groupname_to_gid/0,
            fun readdir/0
        ]}.

new_obj() ->
    ?assertMatch({ok, _}, helpers_nif:new_helper_obj(?DIRECTIO_HELPER_NAME, #{<<"root_path">> => <<"/tmp">>})).

username_to_uid() ->
    ?assertMatch({ok, 0}, helpers_nif:username_to_uid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:username_to_uid(<<"sadmlknfqlwknd">>)).

groupname_to_gid() ->
    ?assertMatch({ok, 0}, helpers_nif:groupname_to_gid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:groupname_to_gid(<<"sadmlknfqlwknd">>)).

readdir() ->
    {ok, Helper} = helpers_nif:new_helper_obj(?DIRECTIO_HELPER_NAME, #{<<"root_path">> => <<"/tmp">>}),
    {ok, Result} = file:list_dir(<<"/tmp">>),
    BinaryResult = lists:map(fun list_to_binary/1, Result),
    {ok, Guard} = helpers_nif:readdir(Helper, <<"">>, 0, 100),
    NifResult =
        receive
            {Guard, Res} ->
                Res
        after 1000 ->
            {error, nif_timeout}
        end,

    ?assertEqual({ok, BinaryResult}, NifResult).

start() ->
    prepare_environment(),
    ok = helpers_nif:init().

stop(_) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets required environment variables.
%% @end
%%--------------------------------------------------------------------
-spec prepare_environment() -> ok.
prepare_environment() ->
    application:set_env(?APP_NAME, ceph_helper_threads_number, 1),
    application:set_env(?APP_NAME, direct_io_helper_threads_number, 1),
    application:set_env(?APP_NAME, s3_helper_threads_number, 1),
    application:set_env(?APP_NAME, swift_helper_threads_number, 1).

-endif.
