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

-include("storage_file_manager_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([gen_status_message/1]).

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
gen_status_message({not_a_space, _}) ->
    #status{code = ?ENOENT, description = describe_error(?ENOENT)};
gen_status_message({not_found, file_meta}) ->
    #status{code = ?ENOENT, description = describe_error(?ENOENT)};
gen_status_message(already_exists) ->
    #status{code = ?EEXIST, description = describe_error(?EEXIST)};
gen_status_message({403,<<>>,<<>>}) ->
    #status{code = ?EACCES, description = describe_error(?EACCES)};
gen_status_message(Error) when is_atom(Error) ->
    case ordsets:is_element(Error, ?ERROR_CODES) of
        true -> #status{code = Error};
        false ->
            #status{code = ?EAGAIN, description = describe_error(Error)}
    end;
gen_status_message({ErrorCode, ErrorDescription}) when
    is_atom(ErrorCode) and is_binary(ErrorDescription) ->
    case ordsets:is_element(ErrorCode, ?ERROR_CODES) of
        true -> #status{code = ErrorCode, description = ErrorDescription};
        false -> #status{code = ?EAGAIN, description = ErrorDescription}
    end;
gen_status_message(Reason) ->
    ?error_stacktrace("Unknown error occured: ~p", [Reason]),
    #status{code = ?EAGAIN, description = <<"An unknown error occured.">>}.

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
