%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides error translators for generic fslogic errors
%% @end
%% ===================================================================
-module(fslogic_errors).
-author("Rafal Slota").

-include("errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([gen_error_message/2, gen_error_code/1, posix_to_oneerror/1, oneerror_to_posix/1]).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Translates given error that was thrown to {ErrorCode :: fslogic_error(), ErrorDetails :: term()}.
%%      This function is intended to be extended when new translation is needed.
%%--------------------------------------------------------------------
-spec gen_error_code(Error :: term()) -> {ErrorCode :: posix_error(), ErrorDetails :: term()}.
gen_error_code({error, Reason}) ->
    gen_error_code(Reason);

%% Generic translations below. All custom translations shall be defined ^above this line.
gen_error_code(ErrorCode) when is_atom(ErrorCode) ->
    case lists:member(ErrorCode, ?ALL_ERROR_CODES) of
        true    -> {ErrorCode, no_details};
        false   -> {?EREMOTEIO, ErrorCode}
    end;
gen_error_code({ErrorCode, ErrorDetails}) when is_atom(ErrorCode) ->
    case lists:member(ErrorCode, ?ALL_ERROR_CODES) of
        true    -> {ErrorCode, ErrorDetails};
        false   -> {?EREMOTEIO, {ErrorCode, ErrorDetails}}
    end;
gen_error_code(UnknownReason) ->
    {?EREMOTEIO, UnknownReason}.


%%--------------------------------------------------------------------
%% @doc Convinience method that returns protobuf answer message that is build base on given error code
%%      and type of request.
%% @end
%%--------------------------------------------------------------------
-spec gen_error_message(RecordName :: atom(), Error :: string()) -> tuple() | no_return().
gen_error_message(test_message_type, Error) ->
    #atom{value = Error};
gen_error_message(RecordName, _Error) ->
    ?error("Unsupported record: ~p", [RecordName]),
    throw({unsupported_record, RecordName}).


%%--------------------------------------------------------------------
%% @doc Translates POSIX error code to internal fslogic_error().
%%--------------------------------------------------------------------
-spec posix_to_oneerror(POSIXErrorCode :: non_neg_integer()) -> ErrorCode :: posix_error().
posix_to_oneerror(POSIX) when POSIX < 0 -> %% All error codes are currently negative, so translate accordingly
    posix_to_oneerror(-POSIX);
posix_to_oneerror(1) ->
    ?EPERM;
posix_to_oneerror(2) ->
    ?ENOENT;
posix_to_oneerror(17) ->
    ?EEXIST;
posix_to_oneerror(13) ->
    ?EACCES;
posix_to_oneerror(122) ->
    ?EDQUOT;
posix_to_oneerror(22) ->
    ?EINVAL;
posix_to_oneerror(39) ->
    ?ENOTEMPTY;
posix_to_oneerror(95) ->
    ?ENOTSUP;
posix_to_oneerror(_Unkwn) ->
    ?EREMOTEIO.


%%--------------------------------------------------------------------
%% @doc Translates internal fslogic_error() to POSIX error code.
%%--------------------------------------------------------------------
-spec oneerror_to_posix(ErrorCode :: posix_error()) -> POSIXErrorCode :: non_neg_integer().
oneerror_to_posix(?OK) ->
    0;
oneerror_to_posix(?EPERM) ->
    1;
oneerror_to_posix(?ENOENT) ->
    2;
oneerror_to_posix(?EEXIST) ->
    17;
oneerror_to_posix(?EACCES) ->
    13;
oneerror_to_posix(?EDQUOT) ->
    122;
oneerror_to_posix(?EINVAL) ->
    22;
oneerror_to_posix(?ENOTEMPTY) ->
    39;
oneerror_to_posix(?ENOTSUP) ->
    95;
oneerror_to_posix(_Unkwn) ->
    121.


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
