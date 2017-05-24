%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for processing logs.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_log).
-author("Tomasz Lichon").

-include("proto/oneclient/proxyio_messages.hrl").

%% API
-export([mask_data_in_message/1]).

-define(MAX_BINARY_DATA_SIZE, 32).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Masks binary data in proxyio messages.
%% @end
%%--------------------------------------------------------------------
-spec mask_data_in_message
    (fslogic_worker:request()) -> fslogic_worker:request();
    (fslogic_worker:response()) -> fslogic_worker:response().
mask_data_in_message(Response = #proxyio_response{
    proxyio_response = RemoteData = #remote_data{
        data = Data
    }
}) when size(Data) > ?MAX_BINARY_DATA_SIZE ->
    Response#proxyio_response{
        proxyio_response = RemoteData#remote_data{data = mask_data(Data)}
    };
mask_data_in_message(Request = #proxyio_request{
    proxyio_request = RemoteWrite = #remote_write{
        byte_sequence = ByteSequences
    }
}) ->
    Request#proxyio_request{proxyio_request = RemoteWrite#remote_write{
        byte_sequence = [Seq#byte_sequence{data = mask_data(Data)}
            || Seq = #byte_sequence{data = Data} <- ByteSequences,
            Data > ?MAX_BINARY_DATA_SIZE
        ]
    }};
mask_data_in_message(Message) ->
    Message.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Shortens binary data into format "<SIZE> bytes", for logging purposes.
%% @end
%%--------------------------------------------------------------------
-spec mask_data(binary()) -> binary().
mask_data(Data) ->
    DataSize = size(Data),
    <<(integer_to_binary(DataSize))/binary, " bytes">>.