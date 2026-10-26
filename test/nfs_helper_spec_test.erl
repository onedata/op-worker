%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of the NFS helper spec built from a create spec. The params it
%%% gives go to the helpers NIF, which takes binaries only: any other value
%%% throws in C++, out of the NIF, and aborts the node.
%%% @end
%%%-------------------------------------------------------------------
-module(nfs_helper_spec_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/nfs.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


helper_params_are_binaries_test() ->
    {ok, HelperParams} = build_helper_params(nfs_configuration(3)),

    ?assertEqual(<<"3">>, maps:get(<<"version">>, HelperParams)),
    ?assertEqual(#{}, maps:filter(fun(_Key, Value) -> not is_binary(Value) end, HelperParams)).


absent_version_is_left_to_the_helper_test() ->
    {ok, HelperParams} = build_helper_params(nfs_configuration(undefined)),

    ?assertNot(maps:is_key(<<"version">>, HelperParams)).


describe_gives_back_the_version_test() ->
    {Configuration, _Credentials} = nfs_helper_spec:describe(helper_spec:build(create_spec(nfs_configuration(4)))),

    ?assertEqual(4, Configuration#nfs_helper_configuration.version).


describe_without_version_gives_undefined_test() ->
    {Configuration, _Credentials} = nfs_helper_spec:describe(helper_spec:build(create_spec(nfs_configuration(undefined)))),

    ?assertEqual(undefined, Configuration#nfs_helper_configuration.version).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
build_helper_params(Configuration) ->
    HelperSpec = helper_spec:build(create_spec(Configuration)),
    helper_spec:build_helper_params(HelperSpec, HelperSpec#helper_spec.credentials).


%% @private
create_spec(Configuration) ->
    #storage_create_spec{
        type = <<"nfs">>,
        name = <<"some-nfs-storage">>,
        timeout = 10000,
        luma = #luma_spec{feed = auto},
        credentials = #nfs_helper_credentials{uid = 0, gid = 0},
        configuration = Configuration
    }.


%% @private
nfs_configuration(Version) ->
    #nfs_helper_configuration{
        version = Version,
        host = <<"nfs.example.com">>,
        volume = <<"/export">>,
        read_ahead = 1024,
        dir_cache = true,
        auto_reconnect = 1,
        connection_pool_size = 10,
        storage_path_type = canonical
    }.
