%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Pre handling of rest request, delegates requests to adequate handlers and
%%% deals with exceptions.
%%% @end
%%%--------------------------------------------------------------------
-module(pre_handler).
-author("Tomasz Lichon").

%% Cowboy handler API
-export([init/3, terminate/3]).

%% Cowboy REST handler API
%% This is not the full cowboy API. If needed, more functions can be added to this bridge.
-export([rest_init/2, malformed_request/2, known_methods/2, allowed_methods/2, is_authorized/2, options/2, resource_exists/2]).
-export([content_types_provided/2, languages_provided/2, charsets_provided/2]).
-export([moved_permanently/2, moved_temporarily/2, content_types_accepted/2, delete_resource/2]).
-export([generate_etag/2, last_modified/2, expires/2, forbidden/2]).

%% Cowboy user defined callbacks
-export([accept_resource/2, provide_resource/2, to_html/2]).

-define(HANDLER_DESCRIPTION_REQUIRED_PROPERTIES, 3).

%%%===================================================================
%%% Cowboy handler API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, called to initialize request handling flow.
%% @end
%%--------------------------------------------------------------------
-spec init(term(), cowboy_req:req(), protocol_plugin_behaviour:handler()) ->
    {upgrade, protocol, cowboy_rest, cowboy_req:req(), term()}.
init(Arg, Req, Description) when is_function(Description) ->
    {HandlerDesc, Req2} = Description(Req),
    init(Arg, Req2, HandlerDesc);
init(Arg, Req, Description)
    when map_size(Description) < ?HANDLER_DESCRIPTION_REQUIRED_PROPERTIES ->
    init(Arg, Req, plugin_properties:fill_with_default(Description));
init(_, Req, #{
    handler := Handler,
    handler_initial_opts := HandlerInitialOpts,
    exception_handler := ExceptionHandler
}) ->
    request_context:set_handler(Handler),
    request_context:set_exception_handler(ExceptionHandler),
    {upgrade, protocol, cowboy_rest, Req, HandlerInitialOpts}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, called after a request is processed.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), Req :: cowboy_req:req(), State :: term()) -> ok.
terminate(Reason, Req, State) ->
    request_delegator:delegate_terminate(Reason, Req, State).

%%%===================================================================
%%% Cowboy REST handler API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, called right after protocol upgrade to init the request context.
%% @end
%%--------------------------------------------------------------------
-spec rest_init(Req :: cowboy_req:req(), Opts :: term()) -> {ok, NewReq :: cowboy_req:req(), State :: term()} | {shutdown, NewReq :: cowboy_req:req()}.
rest_init(Req, HandlerInitialOpts) ->
    request_delegator:delegate_rest_init(Req, HandlerInitialOpts).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Checks request validity.
%% @end
%%--------------------------------------------------------------------
-spec malformed_request(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
malformed_request(Req, State) ->
    request_delegator:delegate(Req, State, malformed_request, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns methods resolvable by the handler.
%% @end
%%--------------------------------------------------------------------
-spec known_methods(Req :: cowboy_req:req(), State :: term()) -> {Result :: [binary()], NewReq :: cowboy_req:req(), NewState :: term()}.
known_methods(Req, State) ->
    request_delegator:delegate(Req, State, known_methods, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function.
%% Returns methods that are allowed, based on version specified in URI.
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(Req :: cowboy_req:req(), State :: term()) -> {Result :: [binary()], NewReq :: cowboy_req:req(), NewState :: term()}.
allowed_methods(Req, State) ->
    request_delegator:delegate(Req, State, allowed_methods, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function.
%% Returns true or false if the client is authorized to perform such request.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
is_authorized(Req, State) ->
    request_delegator:delegate(Req, State, is_authorized, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function.
%% Returns options / requirements associated with a resource.
%% @end
%%--------------------------------------------------------------------
-spec options(Req :: cowboy_req:req(), State :: term()) -> {Result :: [term()], NewReq :: cowboy_req:req(), NewState :: term()}.
options(Req, State) ->
    request_delegator:delegate(Req, State, options, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns content types that can be provided. "application/json" is default.
%% It can be changed later by modifying the header with cowboy_req module.
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(Req :: cowboy_req:req(), State :: term()) -> {Result :: [binary()], NewReq :: cowboy_req:req(), NewState :: term()}.
content_types_provided(Req, State) ->
    request_delegator:delegate_content_types_provided(Req, State).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns list of languages in which the response can be sent.
%% @end
%%--------------------------------------------------------------------
-spec languages_provided(Req :: cowboy_req:req(), State :: term()) -> {Result :: [binary()], NewReq :: cowboy_req:req(), NewState :: term()}.
languages_provided(Req, State) ->
    request_delegator:delegate(Req, State, languages_provided, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns list of charsets in which the response can be encoded.
%% @end
%%--------------------------------------------------------------------
-spec charsets_provided(Req :: cowboy_req:req(), State :: term()) -> {Result :: [binary()], NewReq :: cowboy_req:req(), NewState :: term()}.
charsets_provided(Req, State) ->
    request_delegator:delegate(Req, State, charsets_provided, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns false or {true, Location}.
%% @end
%%--------------------------------------------------------------------
-spec moved_permanently(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
moved_permanently(Req, State) ->
    request_delegator:delegate(Req, State, moved_permanently, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns false or {true, Location}.
%% @end
%%--------------------------------------------------------------------
-spec moved_temporarily(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
moved_temporarily(Req, State) ->
    request_delegator:delegate(Req, State, moved_temporarily, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Determines if resource identified by URI exists.
%% @end
%%--------------------------------------------------------------------
-spec resource_exists(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
resource_exists(Req, State) ->
    request_delegator:delegate(Req, State, resource_exists, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns an etag generated from a static file.
%% @end
%%--------------------------------------------------------------------
-spec generate_etag(Req :: cowboy_req:req(), State :: term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
generate_etag(Req, State) ->
    request_delegator:delegate(Req, State, generate_etag, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns file's modification time.
%% @end
%%--------------------------------------------------------------------
-spec last_modified(Req :: cowboy_req:req(), State :: term()) -> {Result :: integer(), NewReq :: cowboy_req:req(), NewState :: term()}.
last_modified(Req, State) ->
    request_delegator:delegate(Req, State, last_modified, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns time of expiration of a resource.
%% @end
%%--------------------------------------------------------------------
-spec expires(Req :: cowboy_req:req(), State :: term()) -> {Result :: integer(), NewReq :: cowboy_req:req(), NewState :: term()}.
expires(Req, State) ->
    request_delegator:delegate(Req, State, expires, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns true if access to a resource is forbidden.
%% @end
%%--------------------------------------------------------------------
-spec forbidden(Req :: cowboy_req:req(), State :: term()) -> {Result :: boolean(), NewReq :: cowboy_req:req(), NewState :: term()}.
forbidden(Req, State) ->
    request_delegator:delegate(Req, State, forbidden, [Req, State], 2).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(Req :: cowboy_req:req(), State :: term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
content_types_accepted(Req, State) ->
    request_delegator:delegate_content_types_accepted(Req, State).

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Handles DELETE requests.
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(Req :: cowboy_req:req(), State :: term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
delete_resource(Req, State) ->
    request_delegator:delegate(Req, State, delete_resource, [Req, State], 2).

%%%===================================================================
%%% Cowboy user defined callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles incoming resources. Defined in content_types_accepted/2.
%% pre_handler stores user defined callbacks in its context, and this
%% function delegates request to them.
%% @end
%%--------------------------------------------------------------------
-spec accept_resource(cowboy_req:req(), term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
accept_resource(Req, State) ->
    request_delegator:delegate_accept_resource(Req, State).

%%--------------------------------------------------------------------
%% @doc
%% Provides resources to clients. Defined in content_types_provided/2.
%% pre_handler stores user defined callbacks in its context, and this
%% function delegates request to them.
%% @end
%%--------------------------------------------------------------------
-spec provide_resource(cowboy_req:req(), term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
provide_resource(Req, State) ->
    request_delegator:delegate_provide_resource(Req, State).

%%--------------------------------------------------------------------
%% @doc
%% Default provided content type callback.
%% @end
%%--------------------------------------------------------------------
-spec to_html(cowboy_req:req(), term()) -> {Result :: term(), NewReq :: cowboy_req:req(), NewState :: term()}.
to_html(Req, State) ->
    request_delegator:delegate(Req, State, to_html, [Req, State], 2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

