%%%-------------------------------------------------------------------
%%% @author krzysztof
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jul 2014 12:53 AM
%%%-------------------------------------------------------------------
-module(gr_adapter).
-author("krzysztof").

-include("veil_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([set_default_space/2, get_user_spaces/1]).

set_default_space(Space, {UserGID, _AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, #veil_document{record = #user{spaces = Spaces} = UserRec} = UserDoc} ->
            NewSpaces = [Space | lists:delete(Space, Spaces)],
            dao_lib:apply(dao_users, save_user, [UserDoc#veil_document{record = UserRec#user{spaces = NewSpaces}}], 1);
        {error, Reason} ->
            {error, Reason}
    end.

get_user_spaces({UserGID, AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, UserDoc} ->
            #veil_document{record = #user{spaces = Spaces}} = user_logic:synchronize_spaces_info(UserDoc, AccessToken),
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.
