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

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/proxyio_messages.hrl").

%% API
-export([report_file_access_operation/3]).
-export([mask_data_in_message/1]).

-type logged_record() :: fslogic_worker:request() | fuse_request_type() | file_request_type() 
    | provider_request_type() | proxyio_request_type().

-define(MAX_BINARY_DATA_SIZE, 32).

%%%===================================================================
%%% API
%%%===================================================================

-spec report_file_access_operation(fslogic_worker:request(), file_ctx:ctx(), od_user:id()) -> ok.
report_file_access_operation(Request, FileCtx, UserId) ->
    case op_worker:get_env(file_access_audit_log_enabled, false) of
        false -> ok;
        true ->
            FormattedRequest = format_request(Request),
            Uuid = file_ctx:get_logical_uuid_const(FileCtx),
            FormattedShareId = 
                str_utils:to_list(utils:undefined_to_null(file_ctx:get_share_id_const(FileCtx))),
            FilePath = try
                {Path, _} = file_ctx:get_canonical_path(FileCtx),
                Path
            catch _:_ ->
                % could not resolve file path, log anyway
                <<"">>
            end,
            ok = lager:log(file_access_audit_lager_event, info, self(),
                "request: ~s; user: ~s; file_uuid: ~s; share_id: ~s; path: ~ts",
                    [FormattedRequest, UserId, Uuid, FormattedShareId, FilePath]
            )
    end.


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

-spec format_request(logged_record()) -> string().
format_request(#file_request{file_request = SubRecord} = Record) ->
    format_inner_record(Record, SubRecord);
format_request(#fuse_request{fuse_request = SubRecord} = Record) ->
    format_inner_record(Record, SubRecord);
format_request(#provider_request{provider_request = SubRecord} = Record) ->
    format_inner_record(Record, SubRecord);
format_request(#proxyio_request{proxyio_request = SubRecord} = Record) ->
    format_inner_record(Record, SubRecord);
format_request(Record) -> 
    str_utils:format("~tp", [utils:record_type(Record)]).


-spec format_inner_record(logged_record(), logged_record()) -> string().
format_inner_record(Record, SubRecord) ->
    str_utils:format("~tp::~s", [utils:record_type(Record), format_request(SubRecord)]).


%%--------------------------------------------------------------------
%% @doc
%% Shortens binary data into format "<SIZE> bytes", for logging purposes.
%% @end
%%--------------------------------------------------------------------
-spec mask_data(binary()) -> binary().
mask_data(Data) ->
    DataSize = size(Data),
    <<(integer_to_binary(DataSize))/binary, " bytes">>.
