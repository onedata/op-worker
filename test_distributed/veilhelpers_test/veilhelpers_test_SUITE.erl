%% ===================================================================
%% @author Rafał Słota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of veilhelpers module.
%% It contains tests that base on ct.
%% @end
%% ===================================================================

-module(veilhelpers_test_SUITE).

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_vfs.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-define(SH, "DirectIO").
-define(TEST_FILE1, "testfile1").
-define(TEST_FILE2, "testfile2").

-define(O_RDWR, 2). %% fcntl.h: O_RDWR      0x0002      /* open for reading and writing */


-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([integration_test/1]).

all() -> [integration_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Very basic test which checks integration of NIF wrapper with cluster and VeilHelpers library usign DirectIO helper.
%% By calling most of implemented wrapper functions (from veilhelper_nif module) via veilhelpers:exec proxy method, this test checks if: 
%%   - slave nodes are correctly started (otherwise proxy method veilhelpers:exec wouldn't work)
%%   - NIF module is fully loaded on those slave nodes
%%   - Arguments and return values are correctly translated.
integration_test(Config) ->
    NodesUp = ?config(nodes, Config),
    [FSLogicNode | _] = NodesUp,

    SHInfo = #storage_helper_info{name = ?SH, init_args = ?ARG_TEST_ROOT},

    ?assertEqual(0, rpc:call(FSLogicNode, veilhelpers, exec, [mknod, SHInfo, [?TEST_FILE1, 8#744, 0]])),
    ?assertEqual(-17, rpc:call(FSLogicNode, veilhelpers, exec, [mknod, SHInfo, [?TEST_FILE1, 8#744, 0]])),  %% File already exists

    % Open
    {ErrorCode1, FFI} = rpc:call(FSLogicNode, veilhelpers, exec, [open, SHInfo, [?TEST_FILE1, #st_fuse_file_info{flags = ?O_RDWR}]]), %% Open is optional, just for preformance boost
    ?assertEqual(0, ErrorCode1),
    ?assert(FFI#st_fuse_file_info.flags band ?O_RDWR =/= 0),

    % Write
    ErrorCode2 = rpc:call(FSLogicNode, veilhelpers, exec, [write, SHInfo, [?TEST_FILE1, <<"file content">>, 0, FFI]]),
    ?assertEqual(12, ErrorCode2), %% Bytes written count

    % Read
    {ErrorCode4, Data1} = rpc:call(FSLogicNode, veilhelpers, exec, [read, SHInfo, [?TEST_FILE1, 3, 1, FFI]]),
    ?assertEqual(3, ErrorCode4), %% Bytes read count
    ?assertMatch(<<"ile">>, Data1),

    % Close
    ErrorCode3 = rpc:call(FSLogicNode, veilhelpers, exec, [release, SHInfo, [?TEST_FILE2, FFI]]),
    ?assertEqual(0, ErrorCode3),

    % Move file
    ErrorCode5 = rpc:call(FSLogicNode, veilhelpers, exec, [rename, SHInfo, [?TEST_FILE1, ?TEST_FILE2]]),
    ?assertEqual(0, ErrorCode5),

    % Read
    {ErrorCode6, Data2} = rpc:call(FSLogicNode, veilhelpers, exec, [read, SHInfo, [?TEST_FILE2, 4, 4, #st_fuse_file_info{}]]),
    ?assertEqual(4, ErrorCode6), %% Bytes read count
    ?assertMatch(<<" con">>, Data2),

    % getattr
    {ErrorCode7, Stat} = rpc:call(FSLogicNode, veilhelpers, exec, [getattr, SHInfo, [?TEST_FILE2]]),
    ?assertEqual(0, ErrorCode7),
    Mode = Stat#st_stat.st_mode,

    % Chmod
    Mode1 = Mode band (bnot 8#444), %% Revoke read perms for this file
    ErrorCode8 = rpc:call(FSLogicNode, veilhelpers, exec, [chmod, SHInfo, [?TEST_FILE2, Mode1]]),
    ?assertEqual(0, ErrorCode8),

    % getattr
    {ErrorCode9, Stat1} = rpc:call(FSLogicNode, veilhelpers, exec, [getattr, SHInfo, [?TEST_FILE2]]),
    ?assertEqual(0, ErrorCode9),
    ?assertEqual(Mode1, Stat1#st_stat.st_mode),

    % Unlink
    ErrorCode10 = rpc:call(FSLogicNode, veilhelpers, exec, [unlink, SHInfo, [?TEST_FILE2]]),
    ?assertEqual(0, ErrorCode10),

    % mkdir
    ErrorCode11 = rpc:call(FSLogicNode, veilhelpers, exec, [mkdir, SHInfo, [?TEST_FILE1, 8#755]]),
    ?assertEqual(0, ErrorCode11),

    % rmdir
    ErrorCode12 = rpc:call(FSLogicNode, veilhelpers, exec, [rmdir, SHInfo, [?TEST_FILE1]]),
    ?assertEqual(0, ErrorCode12),

    ok.

init_per_testcase(_, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    NodesUp = test_node_starter:start_test_nodes(1),
    [FSLogicNode | _] = NodesUp,

    DB_Node = ?DB_NODE,
    Port = 6666,
    test_node_starter:start_app_on_nodes(?APP_Name, ?VEIL_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [FSLogicNode]}, {dns_port, 1317}, {db_nodes, [DB_Node]},{nif_prefix, './'},{ca_dir, './cacerts/'}]]),

    FSRoot = ?TEST_ROOT,
    file:delete(FSRoot ++ ?TEST_FILE1),
    file:delete(FSRoot ++ ?TEST_FILE2), 

    lists:append([{port, Port}, {nodes, NodesUp}], Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes),
    test_node_starter:stop_deps_for_tester_node().
