%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a custom multipart bridge. It checks if a request is 
%% a multipart POST requests, checks its validity and passes control to file_transfer_handler.
%% @end
%% ===================================================================
-module(veil_multipart_bridge).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% request_cache record for simple_bridge
-record(request_cache, {request, docroot = "", body = ""}).

% Maximum size of posted data in Bytes. Override with multipart_max_length_mb in config
-define(MAX_POST_SIZE, 107374182400).  % 100GB

%% ====================================================================
%% API function
%% ====================================================================
-export([parse/1]).

%% parse/1
%% ====================================================================
%% @doc Try to parse the request encapsulated in request bridge if it is
%% a multipart POST request.
%% @end
-spec parse(ReqBridge :: record()) ->
    {ok, not_multipart} |
    {ok, Params :: [tuple()], Files :: [any()]} |
    {error, any()}.
%% ====================================================================
parse(ReqBridge) ->
    case is_multipart_request(ReqBridge) of
        true ->
            _Result = parse_multipart(ReqBridge);
        false ->
            {ok, not_multipart}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

% Checks if the request encapsulated in request bridge is a multipart request
is_multipart_request(ReqBridge) ->
    try ReqBridge:header(content_type) of
        "multipart/form-data" ++ _ -> true;
        _ -> false
    catch _:_ -> false
    end.


% Checks the validity of multipart request and calls file_transfer_handler to parse it
parse_multipart(ReqBridge) ->
    wf_context:init_context(ReqBridge, undefined),
    wf_handler:call(session_handler, init),
    UserID = try wf:session(user_doc) catch _:_ -> undefined end,

    {_, _, ReqKey, _, _, _, _} = ReqBridge,
    #request_cache{request = Req} = cowboy_request_server:get(ReqKey),
    {LenghtBin, _} = cowboy_req:header(<<"content-length">>, Req),
    Length = list_to_integer(binary_to_list(LenghtBin)),

    case Length > get_max_post_size() of
        true -> throw(post_too_big);
        false -> continue
    end,

    {ok, _Params, _Files} = file_transfer_handler:handle_upload_request(Req, UserID).


% Returns maximum acceptable post size, either default or set in app.src.
get_max_post_size() ->
    case application:get_env(veil_cluster_node, multipart_max_length) of
        {ok, Value} -> Value;
        _ ->
            ?error("Could not read 'multipart_max_length' from config. Make sure it is present in config.yml and .app.src."),
            ?MAX_POST_SIZE
    end.
    