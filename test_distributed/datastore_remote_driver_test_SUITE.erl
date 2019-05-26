%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore remote driver tests.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_remote_driver_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("proto/oneclient/client_messages.hrl").
-include_lib("proto/oneclient/stream_messages.hrl").

%% export for ct
-export([all/0]).

%% tests
-export([
    get_remote_document_msg_should_be_serializable/1,
    remote_document_msg_should_be_serializable/1,
    get_remote_document_should_return_document/1,
    get_remote_document_should_return_missing_error/1,
    get_remote_document_should_return_internal_error/1
]).

all() ->
    ?ALL([
        get_remote_document_msg_should_be_serializable,
        remote_document_msg_should_be_serializable,
        get_remote_document_should_return_document,
        get_remote_document_should_return_missing_error,
        get_remote_document_should_return_internal_error
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_remote_document_msg_should_be_serializable(_Config) ->
    {_, ProtoMsg} = translator:translate_to_protobuf(#get_remote_document{
        model = some_model,
        key = <<"some_key">>,
        routing_key = <<"some_routing_key">>
    }),
    RawMsg = ?assertMatch(<<_/binary>>, messages:encode_msg(ProtoMsg)),
    ?assertMatch(#get_remote_document{}, translator:translate_from_protobuf(
        messages:decode_msg(RawMsg, 'GetRemoteDocument')
    )).

remote_document_msg_should_be_serializable(_Config) ->
    {_, ProtoMsg} = translator:translate_to_protobuf(#remote_document{
        status = #status{code = ?OK},
        compressed_data = <<"some_data">>
    }),
    RawMsg = ?assertMatch(<<_/binary>>, messages:encode_msg(ProtoMsg)),
    ?assertMatch(#remote_document{}, translator:translate_from_protobuf(
        messages:decode_msg(RawMsg, 'RemoteDocument')
    )).

get_remote_document_should_return_document(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, datastore_router),
    test_utils:mock_expect(Worker, datastore_router, route, fun(_, _, _, _) ->
        {ok, #document{
            key = <<"some_key">>,
            value = #custom_metadata{
                space_id = <<"some_id">>,
                file_objectid = <<"some_id">>
            },
            version = 2
        }}
    end),
    ?assertMatch(#remote_document{
        status = #status{code = ?OK},
        compressed_data = <<_/binary>>
    }, rpc:call(Worker, datastore_remote_driver, handle, [#get_remote_document{
        model = custom_metadata,
        key = <<"some_key">>,
        routing_key = <<"some_routing_key">>
    }])),
    test_utils:mock_validate_and_unload(Worker, datastore_router).

get_remote_document_should_return_missing_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, datastore_router),
    test_utils:mock_expect(Worker, datastore_router, route, fun(_, _, _, _) ->
        {error, not_found}
    end),
    ?assertMatch(#remote_document{
        status = #status{code = ?ENOENT}
    }, rpc:call(Worker, datastore_remote_driver, handle, [#get_remote_document{
        model = custom_metadata,
        key = <<"some_key">>,
        routing_key = <<"some_routing_key">>
    }])),
    test_utils:mock_validate_and_unload(Worker, datastore_router).

get_remote_document_should_return_internal_error(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, datastore_router),
    test_utils:mock_expect(Worker, datastore_router, route, fun(_, _, _, _) ->
        {error, internal}
    end),
    ?assertMatch(#remote_document{
        status = #status{code = ?EAGAIN}
    }, rpc:call(Worker, datastore_remote_driver, handle, [#get_remote_document{
        model = custom_metadata,
        key = <<"some_key">>,
        routing_key = <<"some_routing_key">>
    }])),
    test_utils:mock_validate_and_unload(Worker, datastore_router).
