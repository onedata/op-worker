%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2015, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides error translators for generic fslogic errors.
%% @end
%% ===================================================================
-module(fslogic_errors).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([gen_status_message/1, posix_to_internal/1, internal_to_posix/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Translates operation error to status messages.
%%      This function is intended to be extended when new translation is needed.
%%--------------------------------------------------------------------
-spec gen_status_message(Error :: term()) -> #status{}.
gen_status_message({error, Reason}) ->
    gen_status_message(Reason);
gen_status_message({not_found, file_meta}) ->
    #status{code = ?ENOENT, description = describe_error(?ENOENT)};
gen_status_message(already_exists) ->
    #status{code = ?EEXIST, description = describe_error(?EEXIST)};
gen_status_message(Error) when is_atom(Error) ->
    case lists:member(Error, ?ERROR_CODES) of
        true -> #status{code = Error};
        false -> #status{code = ?EAGAIN, description = describe_error(Error)}
    end;
gen_status_message({ErrorCode, ErrorDescription}) when
    is_atom(ErrorCode) and is_binary(ErrorDescription) ->
    case lists:member(ErrorCode, ?ERROR_CODES) of
        true -> #status{code = ErrorCode, description = ErrorDescription};
        false -> #status{code = ?EAGAIN, description = ErrorDescription}
    end;
gen_status_message(Reason) ->
    ?error("Unknown error occured: ~p", [Reason]),
    #status{code = ?EAGAIN, description = <<"An unknown error occured.">>}.

%%--------------------------------------------------------------------
%% @doc Translates POSIX error code to internal error code.
%% @end
%%--------------------------------------------------------------------
-spec posix_to_internal(POSIXErrorCode :: integer()) ->
    InternalErrorCode :: code().
posix_to_internal(Code) when is_integer(Code), Code < 0 ->
    posix_to_internal(Code * -1);
posix_to_internal(1) ->
    ?EPERM;
posix_to_internal(2) ->
    ?ENOENT;
posix_to_internal(17) ->
    ?EEXIST;
posix_to_internal(13) ->
    ?EACCES;
posix_to_internal(122) ->
    ?EDQUOT;
posix_to_internal(22) ->
    ?EINVAL;
posix_to_internal(39) ->
    ?ENOTEMPTY;
posix_to_internal(95) ->
    ?ENOTSUP;
posix_to_internal(_) ->
    ?EAGAIN.

%%--------------------------------------------------------------------
%% @doc Translates internal error code to POSIX error code.
%% @end
%%--------------------------------------------------------------------
-spec internal_to_posix(InternalErrorCode :: code()) ->
    POSIXErrorCode :: non_neg_integer().
internal_to_posix(?OK) ->
    0;
internal_to_posix(?EPERM) ->
    1;
internal_to_posix(?ENOENT) ->
    2;
internal_to_posix(?EEXIST) ->
    17;
internal_to_posix(?EACCES) ->
    13;
internal_to_posix(?EDQUOT) ->
    122;
internal_to_posix(?EINVAL) ->
    22;
internal_to_posix(?ENOTEMPTY) ->
    39;
internal_to_posix(?ENOTSUP) ->
    95;
internal_to_posix(_) ->
    121.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates error ID to error description.
%% @end
%%--------------------------------------------------------------------
-spec describe_error(ErrorId :: atom()) -> ErrorDescription :: binary().
describe_error(ErrorId) ->
    case lists:keyfind(ErrorId, 1, ?ERROR_DESCRIPTIONS) of
        {ErrorId, ErrorDescription} -> ErrorDescription;
        false -> atom_to_binary(ErrorId, utf8)
    end.