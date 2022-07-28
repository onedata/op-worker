%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating CDMI file metadata in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_cdmi).
-author("Bartosz Walkowicz").

%% API
-export([
    get_transfer_encoding/3,
    set_transfer_encoding/4,

    get_cdmi_completion_status/3,
    set_cdmi_completion_status/4,

    get_mimetype/3,
    set_mimetype/4
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_cdmi, ?FUNCTION_NAME, Args) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_transfer_encoding(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, custom_metadata:transfer_encoding()} | errors:error().
get_transfer_encoding(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec set_transfer_encoding(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    custom_metadata:transfer_encoding()
) ->
    ok | errors:error().
set_transfer_encoding(NodeSelector, SessionId, FileKey, Encoding) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Encoding]).


-spec get_cdmi_completion_status(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, custom_metadata:cdmi_completion_status()} | errors:error().
get_cdmi_completion_status(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec set_cdmi_completion_status(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    custom_metadata:cdmi_completion_status()
) ->
    ok | errors:error().
set_cdmi_completion_status(NodeSelector, SessionId, FileKey, CompletionStatus) ->
    ?CALL(NodeSelector, [SessionId, FileKey, CompletionStatus]).


-spec get_mimetype(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, custom_metadata:mimetype()} | errors:error().
get_mimetype(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec set_mimetype(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    custom_metadata:mimetype()
) ->
    ok | errors:error().
set_mimetype(NodeSelector, SessionId, FileKey, Mimetype) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Mimetype]).
