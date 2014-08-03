%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convinience functions designed for 
%% REST handling modules.
%% @end
%% ===================================================================

-module(rest_utils).

-include_lib("public_key/include/public_key.hrl").
-include("err.hrl").
-include("veil_modules/control_panel/common.hrl").

-export([map/2, unmap/3, encode_to_json/1, decode_from_json/1]).
-export([success_reply/1, error_reply/1]).
-export([verify_peer_cert/1, prepare_context/1, reply_with_error/4, join_to_path/1, list_dir/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% map/2
%% ====================================================================
%% @doc Converts a record to JSON conversion-ready tuple list.
%% For this input:
%% RecordToMap = #some_record{id=123, message="mess"}
%% Fields = [id, message]
%% The function will produce: [{id, 123},{message, "mess"}]
%% @end
-spec map(record(), [atom()]) -> [tuple()].
%% ====================================================================
map(RecordToMap, Fields) ->
    Y = [try N = lists:nth(1, B), if is_number(N) -> gui_str:to_binary(B); true -> B end catch _:_ -> B end
        || B <- tl(tuple_to_list(RecordToMap))],
    lists:zip(Fields, Y).


%% unmap/3
%% ====================================================================
%% @doc Converts a tuple list resulting from JSON to erlang translation
%% into an erlang record. Reverse process to map/2.
%% For this input: 
%% Proplist = [{id, 123},{message, "mess"}]
%% RecordTuple = #some_record{}
%% Fields = [id, message]
%% The function will produce: #some_record{id=123, message="mess"}
%% @end
-spec unmap([tuple()], record(), [atom()]) -> [tuple()].
%% ====================================================================
unmap([], RecordTuple, _) ->
    RecordTuple;

unmap([{KeyBin, Val} | Proplist], RecordTuple, Fields) ->
    Key = binary_to_atom(KeyBin, latin1),
    Value = case Val of
                I when is_integer(I) -> Val;
                A when is_atom(A) -> Val;
                _ -> gui_str:to_list(Val)
            end,
    Index = string:str(Fields, [Key]) + 1,
    true = (Index > 1),
    unmap(Proplist, setelement(Index, RecordTuple, Value), Fields).


%% encode_to_json/1
%% ====================================================================
%% @doc Convinience function that convert an erlang term to JSON, producing
%% binary result. The output is in UTF8 encoding.
%%
%% Possible terms, can be nested:
%% {struct, Props} - Props is a structure as a proplist, e.g.: [{id, 13}, {message, "mess"}]
%% {Props} - alias for above
%% {array, Array} - Array is a list, e.g.: [13, "mess"]
%% @end
-spec encode_to_json(term()) -> binary().
%% ====================================================================
encode_to_json(Term) ->
    Encoder = mochijson2:encoder([{utf8, true}]),
    iolist_to_binary(Encoder(Term)).


%% decode_from_json/1
%% ====================================================================
%% @doc Convinience function that convert JSON binary to an erlang term.
%% @end
-spec decode_from_json(term()) -> binary().
%% ====================================================================
decode_from_json(JSON) ->
    mochijson2:decode(JSON).


%% success_reply/1
%% ====================================================================
%% @doc Produces a standarized JSON return message, indicating success of an operation.
%% It can be inserted directly into response body. Macros from rest_messages.hrl should
%% be used as an argument to this function.
%% @end
-spec success_reply(binary()) -> binary().
%% ====================================================================
success_reply({Code, Message}) ->
    <<"{\"status\":\"ok\",\"code\":\"", Code/binary, "\",\"description\":\"", Message/binary, "\"}">>.


%% error_reply/1
%% ====================================================================
%% @doc Produces a standarized JSON return message, indicating failure of an operation.
%% It can be inserted directly into response body. #error_rec{} from err.hrl should
%% be used as an argument to this function.
%% @end
-spec error_reply(#error_rec{}) -> binary().
%% ====================================================================
error_reply(Record) ->
    rest_utils:encode_to_json({struct, rest_utils:map(Record, [status, code, description])}).


%% verify_peer_cert/1
%% ====================================================================
%% @doc Verifies peer certificate (obtained from given cowboy request)
%% @end
-spec verify_peer_cert(Req :: req()) -> {ok, DnString :: string()} | no_return().
%% ====================================================================
verify_peer_cert(Req) ->
    {OtpCert, Certs} = try
                           {ok, PeerCert} = ssl:peercert(cowboy_req:get(socket, Req)),
                           {ok, {Serial, Issuer}} = public_key:pkix_issuer_id(PeerCert, self),
                           [{_, [TryOtpCert | TryCerts], _}] = ets:lookup(gsi_state, {Serial, Issuer}),
                           {TryOtpCert, TryCerts}
                       catch
                           _:_ ->
                               ?error("[REST] Peer connected but cerificate chain was not found. Please check if GSI validation is enabled."),
                               erlang:error(invalid_cert)
                       end,
    case gsi_handler:call(gsi_nif, verify_cert_c,
        [public_key:pkix_encode('OTPCertificate', OtpCert, otp),                    %% peer certificate
            [public_key:pkix_encode('OTPCertificate', Cert, otp) || Cert <- Certs], %% peer CA chain
            [DER || [DER] <- ets:match(gsi_state, {{ca, '_'}, '$1', '_'})],         %% cluster CA store
            [DER || [DER] <- ets:match(gsi_state, {{crl, '_'}, '$1', '_'})]]) of    %% cluster CRL store
        {ok, 1} ->
            {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
            {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
            {ok,_DnString} = user_logic:rdn_sequence_to_dn_string(Rdn);
        {ok, 0, Errno} ->
            ?info("[REST] Peer ~p was rejected due to ~p error code", [OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject, Errno]),
            erlang:error({gsi_error_code, Errno});
        {error, Reason} ->
            ?error("[REST] GSI peer verification callback error: ~p", [Reason]),
            erlang:error(Reason);
        Other ->
            ?error("[REST] GSI verification callback returned unknown response ~p", [Other]),
            erlang:error({gsi_unknown_response, Other})
    end.


%% prepare_context/1
%% ====================================================================
%% @doc This function attempts to get user (with given DN) from db, and to put him into fslogic_context
%% @end
-spec prepare_context(DnString :: string()) -> ok | {error, {user_unknown, DnString :: string()}}.
%% ====================================================================
prepare_context(DnString) ->
    case user_logic:get_user({dn, DnString}) of
        {ok, _} ->
            fslogic_context:set_user_dn(DnString),
            ?info("[REST] Peer connected using certificate with subject: ~p ~n", [DnString]),
            ok;
        _ -> {error, {user_unknown, DnString}}
    end.


%% reply_with_error/4
%% ====================================================================
%% Replies with 500 error cose, content-type set to application/json and
%% an error message
%% @end
-spec reply_with_error(req(), atom(), {string(), string()}, list()) -> req().
%% ====================================================================
reply_with_error(Req, Severity, ErrorDesc, Args) ->
    ErrorRec = case Severity of
                   warning -> ?report_warning(ErrorDesc, Args);
                   error -> ?report_error(ErrorDesc, Args);
                   alert -> ?report_alert(ErrorDesc, Args)
               end,
    Req2 = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), Req),
    {ok, Req3} = cowboy_req:reply(500, Req2),
    Req3.


%% join_to_path/1
%% ====================================================================
%% @doc
%% This function joins a list of binaries with slashes so they represent a filepath.
%% @end
-spec join_to_path([binary()]) -> binary().
%% ====================================================================
join_to_path([Binary|Tail]) ->
    join_to_path(Binary, Tail).

join_to_path(Path, []) ->
    Path;

join_to_path(Path, [Binary|Tail]) ->
    join_to_path(<<Path/binary, "/", Binary/binary>>, Tail).


%% list_dir/1
%% ====================================================================
%% @doc List the given directory, calling itself recursively if there is more to fetch.
%% @end
-spec list_dir(string()) -> [string()].
%% ====================================================================
list_dir(Path) ->
    list_dir(Path, 0, 10, []).

list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.