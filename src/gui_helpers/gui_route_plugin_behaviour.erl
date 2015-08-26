%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Aug 2015 13:27
%%%-------------------------------------------------------------------
-module(gui_route_plugin_behaviour).
-author("lopiola").

-include("gui.hrl").


%%--------------------------------------------------------------------
%% @doc
%% Should return a gui_route record per every page that a user can visit.
%% It is recommended to return a 404 page if the Path does not match
%% any existing page.
%% @end
%%--------------------------------------------------------------------
-callback route(Path :: binary()) -> #gui_route{}.


%%--------------------------------------------------------------------
%% @doc
%% Should return login page where the user will be redirected if he requests
%% a page that can only be visited when logged in.
%% @end
%%--------------------------------------------------------------------
-callback login_page() -> Page :: binary().


%%--------------------------------------------------------------------
%% @doc
%% Should return a default page where the user will be redirected if
%% he requests a page that he cannot currently visit (for example login page
%% when the user is already logged in).
%% @end
%%--------------------------------------------------------------------
-callback default_page() -> Page :: binary().

