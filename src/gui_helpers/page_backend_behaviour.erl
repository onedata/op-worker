%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Aug 2015 13:27
%%%-------------------------------------------------------------------
-module(page_backend_behaviour).
-author("lopiola").

%% API

%%--------------------------------------------------------------------
%% @doc
%% Called before the page (HTML) is served. Used to decide how to
%% respond to page query (see returned values).
%% @end
%%--------------------------------------------------------------------
-callback page_init() ->
    % Will serve the HTML file defined in ?GUI_ROUTE_PLUGIN
    serve_html |
    % Same as above, adding given headers to default ones
    {serve_html, Headers} |
    % Will serve explicite body
    {serve_body, Body} |
    % Same as above, with given headers
    {serve_body, Body, Headers} |
    % Will reply with given code, body and headers
    {reply, Code, Body, Headers} |
    % Will send a 307 redirect back to the client
    {redirect, URL} when
    Code :: integer(),
    Body :: binary(),
    Headers :: [{binary(), binary()}],
    URL :: binary().

