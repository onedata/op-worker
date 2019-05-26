%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions delegating request to appropriate handlers.
%%% @end
%%%--------------------------------------------------------------------
-module(request_delegator).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% API
-export([delegate_terminate/3,
    delegate_content_types_provided/2, delegate_content_types_accepted/2,
    delegate_accept_resource/2, delegate_provide_resource/2, delegate/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Delegates terminate function
%% @end
%%--------------------------------------------------------------------
-spec delegate_terminate(term(), cowboy_req:req(), term()) -> term().
delegate_terminate(Reason, Req, State) ->
    call_and_handle_exception(Req, State, terminate, [Reason, Req, State]).

%%--------------------------------------------------------------------
%% @doc
%% Delegates content_types_provided function, analyses response and stores
%% it in context, then replaces each callback with 'provide_resource' function
%% which routes requests later on basing on stored context.
%% @end
%%--------------------------------------------------------------------
-spec delegate_content_types_provided(term(), cowboy_req:req()) -> term().
delegate_content_types_provided(Req, State) ->
    case delegate(Req, State, content_types_provided, [Req, State], 2) of
        {Provided, Req, State} ->
            request_context:set_content_types_provided(Provided),
            {change_callbacks(Provided, provide_resource), Req, State};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% %% Delegates content_types_accepted function, analyses response and stores
%% it in context, then replaces each callback with 'accept_resource' function
%% which routes requests later on basing on stored context.
%% @end
%%--------------------------------------------------------------------
-spec delegate_content_types_accepted(cowboy_req:req(), term()) -> term().
delegate_content_types_accepted(Req, State) ->
    case delegate(Req, State, content_types_accepted, [Req, State], 2) of
        {Accepted, Req, State} ->
            request_context:set_content_types_accepted(Accepted),
            {change_callbacks(Accepted, accept_resource), Req, State};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Routes resource request to adequate callback basing on context.
%% @end
%%--------------------------------------------------------------------
-spec delegate_provide_resource(cowboy_req:req(), term()) -> term().
delegate_provide_resource(Req, State) ->
    try
        {ok, {Req2, Callback}} = plugin_callback_selector:select_provide_callback(Req),
        delegate(Req2, State, Callback, [Req, State], 2)
    catch
        T:E ->
            erlang:apply(request_context:get_exception_handler(), [Req, State, T, E])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Routes request with incoming resource to adequate callback basing
%% on context.
%% @end
%%--------------------------------------------------------------------
-spec delegate_accept_resource(cowboy_req:req(), term()) -> term().
delegate_accept_resource(Req, State) ->
    try
        {ok, {Req2, Callback}} = plugin_callback_selector:select_accept_callback(Req),
        delegate(Req2, State, Callback, [Req, State], 2)
    catch
        T:E ->
            erlang:apply(request_context:get_exception_handler(), [Req, State, T, E])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Function used to delegate a cowboy callback.
%% If handler implements callback - it is called, otherwise the default
%% cowboy callback is used.
%% @end
%%--------------------------------------------------------------------
-spec delegate(cowboy_req:req(), term(), atom(), [term()],
  integer()) -> term().
delegate(Req, State, Fun, Args, Arity) ->
    case erlang:function_exported(request_context:get_handler(), Fun, Arity) of
        false ->
            no_call;
        true ->
            call_and_handle_exception(Req, State, Fun, Args)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------
-spec call_and_handle_exception(cowboy_req:req(), term(), atom(), [term()]) -> term().
call_and_handle_exception(Req, State, Fun, Args) ->
    try
        erlang:apply(request_context:get_handler(), Fun, Args)
    catch
        T:E ->
            erlang:apply(request_context:get_exception_handler(), [Req, State, T, E])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Changes all handlers to given one.
%% @end
%%--------------------------------------------------------------------
-spec change_callbacks([plugin_callback_selector:content_type_callback()], atom()) ->
    [plugin_callback_selector:content_type_callback()].
change_callbacks([], _Callback) ->
    [];
change_callbacks([{Type, _Handler} | Rest], Callback) ->
    [{Type, Callback} | change_callbacks(Rest, Callback)].
