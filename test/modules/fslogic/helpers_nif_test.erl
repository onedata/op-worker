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

new_obj_test() ->
    prepare_environment(),
    ok = helpers_nif:init(),
    ?assertMatch({ok, _}, helpers_nif:new_helper_obj(?DIRECTIO_HELPER_NAME, #{<<"root_path">> => <<"/tmp">>})),
    ok.

username_to_uid_test() ->
    prepare_environment(),
    ok = helpers_nif:init(),
    ?assertMatch({ok, 0}, helpers_nif:username_to_uid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:username_to_uid(<<"sadmlknfqlwknd">>)),
    ok.

groupname_to_gid_test() ->
    prepare_environment(),
    ok = helpers_nif:init(),
    ?assertMatch({ok, 0}, helpers_nif:groupname_to_gid(<<"root">>)),
    ?assertMatch({error, einval}, helpers_nif:groupname_to_gid(<<"sadmlknfqlwknd">>)),
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
