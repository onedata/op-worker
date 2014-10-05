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
<<<<<<< HEAD:src/veil_modules/control_panel/utils/rest_utils.erl
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_error.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-export([map/2, unmap/3, encode_to_json/1, decode_from_json/1]).
-export([success_reply/1, error_reply/1]).
-export([verify_peer_cert/1, prepare_context/1, reply_with_error/4, join_to_path/1, parse_body/1,
         validate_body/1, ensure_path_ends_with_slash/1, get_path_leaf_with_ending_slash/1]).
=======
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/control_panel/cdmi_error.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

-export([map/2, unmap/3, encode_to_json/1, decode_from_json/1]).
-export([success_reply/1, error_reply/1]).
-export([verify_peer_cert/2, prepare_context/1, reply_with_error/4, join_to_path/1, list_dir/1, parse_body/1,
         validate_body/1, ensure_path_ends_with_slash/1, get_path_leaf_with_ending_slash/1, trim_spaces/1]).
>>>>>>> develop:src/oneprovider_modules/control_panel/utils/rest_utils.erl

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
    Key = binary_to_existing_atom(KeyBin, latin1),
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
-spec decode_from_json(binary()) -> term().
%% ====================================================================
decode_from_json(JSON) ->
    try mochijson2:decode(JSON, [{format, proplist}]) catch _:_ -> throw(?invalid_json) end.


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
-spec verify_peer_cert(Req :: req(), Certs :: false | {certs, {PeerCert :: term(), Chain :: [term()]}}) -> {ok, DnString :: string()} | no_return().
%% ====================================================================
verify_peer_cert(Req, GSIState) ->
    case cowboy_req:header(<<"x-auth-token">>, Req) of
        {undefined, Req1} ->
            case GSIState of
                {certs, {OtpCert, Certs}} ->
                    {ok, EEC} = gsi_handler:find_eec_cert(OtpCert, Certs, gsi_handler:is_proxy_certificate(OtpCert)),
                    {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
                    {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
                    {ok, DnString, Req1};
                Unknown ->
                    ?error("[REST] Peer connected but cerificate chain was not found (find result: ~p). Please check if GSI validation is enabled.", [Unknown]),
                     throw(?invalid_cert)
            end;
        {Token, Req2} ->
            case binary:split(base64:decode(Token),<<";">>) of
                [AccessToken, GRUID] ->
                    {ok, {token, AccessToken, GRUID}, Req2};
                _ ->
                    ?error("[REST] Peer was rejected due to invalid token format"),
                    throw(?invalid_token)
            end
    end.


%% prepare_context/1
%% ====================================================================
%% @doc This function attempts to get user (with given DN) from db, and to put him into fslogic_context
%% @end
-spec prepare_context(Identity) -> Result when
    Identity :: DnString | Token,
    DnString :: string(),
    Token :: {token, AccessToken :: binary(), GRUID :: binary()},
    Result :: ok | {error, {?user_unknown, DnString :: string()}}.
%% ====================================================================
prepare_context({token, Token, GRUID}) ->
    fslogic_context:set_gr_auth(GRUID,Token),
    ok;
prepare_context(DnString) ->
    case user_logic:get_user({dn, DnString}) of
        {ok, _} ->
            fslogic_context:set_user_dn(DnString),
            ?info("[REST] Peer connected using certificate with subject: ~p ~n", [DnString]),
            ok;
        _ -> {error, {?user_unknown, DnString}}
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

%% parse_body/1
%% ====================================================================
%% @doc Parses json request body to erlang proplist format.
%% @end
-spec parse_body(binary()) -> list().
%% ====================================================================
parse_body(RawBody) ->
    case gui_str:binary_to_unicode_list(RawBody) of
        "" -> [];
        NonEmptyBody -> rest_utils:decode_from_json(gui_str:binary_to_unicode_list(NonEmptyBody))
    end.

%% validate_body/1
%% ====================================================================
%% @doc Checks if body contains unique opts.
%% @end
-spec validate_body(Body :: list()) -> ok | no_return().
%% ====================================================================
validate_body(Body) ->
    Keys = proplists:get_keys(Body),
    KeySet = sets:from_list(Keys),
    ExclusiveRequiredKeysSet = sets:from_list(?keys_required_to_be_exclusive),
    case length(Keys) =:= length(Body) of
        true ->
            case sets:size(sets:intersection(KeySet, ExclusiveRequiredKeysSet)) of
                N when N > 1 -> throw(?conflicting_body_fields);
                _ -> ok
            end;
        false -> throw(?duplicated_body_fields)
    end.

%% ensure_path_ends_with_slash/1
%% ====================================================================
%% @doc Appends '/' to the end of filepath if last character is different
-spec ensure_path_ends_with_slash(string()) -> string().
%% ====================================================================
ensure_path_ends_with_slash([]) ->
    "/";
ensure_path_ends_with_slash(Path) ->
    case lists:last(Path) of
        $/ -> Path;
        _ -> Path ++ "/"
    end.

%% get_path_leaf_with_ending_slash/1
%% ====================================================================
%% @doc Get filepath leaf with '/' at the end
-spec get_path_leaf_with_ending_slash(string()) -> string().
%% ====================================================================
get_path_leaf_with_ending_slash(Path) ->
    ensure_path_ends_with_slash(fslogic_path:basename(Path)).

