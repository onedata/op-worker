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
-include("veil_modules/dao/dao_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send_req/3, send_req/4, send_req/5]).
-export([set_default_space/2, join_space/2, leave_space/2, create_space/2, delete_space/2]).
-export([get_space_info/2, get_user_spaces/1, request_support/2, change_space_name/3]).
-export([get_space_providers/2, get_provider_details/3]).
-export([get_space_users/2, get_user_details/3, invite_user/2]).
-export([get_space_groups/2, get_group_details/3]).
-export([get_access_code/1, get_tokens/1, delete_token/2]).

%% send_req/2
%% ====================================================================
%% @doc Sends given request to Global Registry with default options
%% and empty body using REST API.
-spec send_req(Uri :: string(), Method :: atom(), AccessToken :: term()) -> Result when
    Result :: {ok, Status :: string(), ResponseHeaders :: binary(), ResponseBody :: binary()} | {error, Reason :: term()}.
%% ====================================================================
send_req(Uri, Method, {UserGID, AccessToken}) ->
    send_req(Uri, Method, [], {UserGID, AccessToken}).


%% send_req/3
%% ====================================================================
%% @doc Sends given request to Global Registry with default options
%% using REST API.
-spec send_req(Uri :: string(), Method :: atom(), Body :: binary(), AccessToken :: binary()) -> Result when
    Result :: {ok, Status :: string(), ResponseHeaders :: binary(), ResponseBody :: binary()} | {error, Reason :: term()}.
%% ====================================================================
send_req(Uri, Method, Body, {_UserGID, AccessToken}) ->
    send_req(Uri, Method, Body, [], {_UserGID, AccessToken}).


%% send_req/5
%% ====================================================================
%% @doc Sends given request to Global Registry using REST API.
-spec send_req(Uri :: string(), Method :: atom(), Body :: binary(), Options :: list(), AccessToken :: term()) -> Result when
    Result :: {ok, Status :: string(), ResponseHeaders :: binary(), ResponseBody :: binary()} | {error, Reason :: term()}.
%% ====================================================================
send_req(Uri, Method, Body, Options, {_UserGID, AccessToken}) ->
    Url = "https://onedata.org:8443",
    KeyFile = "./certs/grpkey.pem",
    CertFile = "./certs/grpcert.pem",
    CACertFile = "./certs/grpca.pem",
    {ok, Key} = file:read_file(KeyFile),
    {ok, Cert} = file:read_file(CertFile),
    {ok, CACert} = file:read_file(CACertFile),
    [{KeyType, KeyEncoded, _} | _] = public_key:pem_decode(Key),
    [{_, CertEncoded, _} | _] = public_key:pem_decode(Cert),
    [{_, CACertEncoded, _} | _] = public_key:pem_decode(CACert),
    SSLOptions = {ssl_options, [{cacerts, [CACertEncoded]}, {key, {KeyType, KeyEncoded}}, {cert, CertEncoded}]},
    ibrowse:send_req(Url ++ Uri, [{content_type, "application/json"},
        {"authorization", binary_to_list(<<"Bearer ", AccessToken/binary>>)}], Method, Body, [SSLOptions | Options]).


set_default_space(Space, {UserGID, _AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, #veil_document{record = #user{spaces = Spaces} = UserRec} = UserDoc} ->
            NewSpaces = [Space | lists:delete(Space, Spaces)],
            dao_lib:apply(dao_users, save_user, [UserDoc#veil_document{record = UserRec#user{spaces = NewSpaces}}], 1);
        {error, Reason} ->
            {error, Reason}
    end.


create_space(Name, {UserGID, AccessToken}) ->
    try
        Uri = "/user/spaces",
        Body = iolist_to_binary(mochijson2:encode({struct, [{<<"name">>, Name}]})),
        {ok, "201", ResHeaders, _ResBody} = send_req(Uri, post, Body, {UserGID, AccessToken}),
        <<"/spaces/", SpaceId/binary>> = list_to_binary(proplists:get_value("location", ResHeaders)),
        {ok, #veil_document{record = #user{spaces = Spaces} = UserRec} = UserDoc} = user_logic:get_user({global_id, UserGID}),
        NewSpaces = Spaces ++ [SpaceId],
        {ok, _} = dao_lib:apply(dao_users, save_user, [UserDoc#veil_document{record = UserRec#user{spaces = NewSpaces}}], 1),
        {ok, SpaceId}
    catch
        _:Reason ->
            ?error("Cannot create Space with name ~p: ~p", [Name, Reason]),
            {error, Reason}
    end.


join_space(Token, {UserGID, AccessToken}) ->
    try
        Uri = "/user/spaces/join",
        Body = iolist_to_binary(mochijson2:encode({struct, [{<<"token">>, Token}]})),
        {ok, "201", ResHeaders, _ResBody} = send_req(Uri, post, Body, {UserGID, AccessToken}),
        <<" /user/spaces/", SpaceId/binary>> = list_to_binary(proplists:get_value("location", ResHeaders)),
        {ok, #veil_document{record = #user{spaces = Spaces} = UserRec} = UserDoc} = user_logic:get_user({global_id, UserGID}),
        NewSpaces = Spaces ++ [SpaceId],
        {ok, _} = dao_lib:apply(dao_users, save_user, [UserDoc#veil_document{record = UserRec#user{spaces = NewSpaces}}], 1),
        {ok, SpaceId}
    catch
        _:Reason ->
            ?error("Cannot join Space using token ~p: ~p", [Token, Reason]),
            {error, Reason}
    end.


leave_space(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = binary_to_list(<<"/user/spaces/", SpaceId/binary>>),
        {ok, "204", _ResHeaders, _ResBody} = send_req(Uri, delete, {UserGID, AccessToken}),
        ok
    catch
        _:Reason ->
            ?error("Cannot leave Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


delete_space(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = binary_to_list(<<"/spaces/", SpaceId/binary>>),
        {ok, "204", _ResHeaders, _ResBody} = send_req(Uri, delete, {UserGID, AccessToken}),
        ok
    catch
        _:Reason ->
            ?error("Cannot delete Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


get_space_info(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/user/spaces/" ++ binary_to_list(SpaceId),
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        List = mochijson2:decode(ResBody, [{format, proplist}]),
        Name = proplists:get_value(<<"name">>, List),
        true = (Name =/= undefiend),
        {ok, #space_info{space_id = SpaceId, name = Name}}
    catch
        _:Reason ->
            ?error("Cannot get details of Space with ID ~p: ~p", [SpaceId, Reason]),
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


get_space_providers(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/providers",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        List = mochijson2:decode(ResBody, [{format, proplist}]),
        Providers = proplists:get_value(<<"providers">>, List),
        true = (Providers =/= undefiend),
        {ok, Providers}
    catch
        _:Reason ->
            ?error("Cannot get providers of Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


get_provider_details(SpaceId, ProviderId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/providers/" ++ binary_to_list(ProviderId),
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        List = mochijson2:decode(ResBody, [{format, proplist}]),
        Urls = proplists:get_value(<<"urls">>, List),
        true = (Urls =/= undefiend),
        RedirectionPoint = proplists:get_value(<<"redirectionPoint">>, List),
        true = (RedirectionPoint =/= undefiend),
        {ok, {ProviderId, Urls, RedirectionPoint}}
    catch
        _:Reason ->
            ?error("Cannot get provider's details for provider with ID ~p: ~p", [ProviderId, Reason]),
            {error, Reason}
    end.


get_space_users(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/users",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        List = mochijson2:decode(ResBody, [{format, proplist}]),
        Users = proplists:get_value(<<"users">>, List),
        true = (Users =/= undefiend),
        {ok, Users}
    catch
        _:Reason ->
            ?error("Cannot get users of Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


get_user_details(SpaceId, UserId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/users/" ++ binary_to_list(UserId),
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        Name = proplists:get_value(<<"name">>, mochijson2:decode(ResBody, [{format, proplist}])),
        true = (Name =/= undefiend),
        {ok, {UserId, Name}}
    catch
        _:Reason ->
            ?error("Cannot get provider's details for user with ID ~p: ~p", [UserId, Reason]),
            {error, Reason}
    end.


get_space_groups(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/groups",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        List = mochijson2:decode(ResBody, [{format, proplist}]),
        Groups = proplists:get_value(<<"groups">>, List),
        true = (Groups =/= undefiend),
        {ok, Groups}
    catch
        _:Reason ->
            ?error("Cannot get users of Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


get_group_details(SpaceId, GroupId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/groups/" ++ binary_to_list(GroupId),
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        Name = proplists:get_value(<<"name">>, mochijson2:decode(ResBody, [{format, proplist}])),
        true = (Name =/= undefiend),
        {ok, {GroupId, Name}}
    catch
        _:Reason ->
            ?error("Cannot get provider's details for user with ID ~p: ~p", [GroupId, Reason]),
            {error, Reason}
    end.

request_support(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/providers/token",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        Token = proplists:get_value(<<"token">>, mochijson2:decode(ResBody, [{format, proplist}])),
        true = (Token =/= undefiend),
        {ok, Token}
    catch
        _:Reason ->
            ?error("Cannot get support token for Space ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.

invite_user(SpaceId, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId) ++ "/users/token",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        Token = proplists:get_value(<<"token">>, mochijson2:decode(ResBody, [{format, proplist}])),
        true = (Token =/= undefiend),
        {ok, Token}
    catch
        _:Reason ->
            ?error("Cannot get invitation token for Space ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.

change_space_name(SpaceId, Name, {UserGID, AccessToken}) ->
    try
        Uri = "/spaces/" ++ binary_to_list(SpaceId),
        Body = iolist_to_binary(mochijson2:encode({struct, [{<<"name">>, Name}]})),
        {ok, "204", _ResHeaders, _ResBody} = send_req(Uri, patch, Body, {UserGID, AccessToken}),
        ok
    catch
        _:Reason ->
            ?error("Cannot change Space name for Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.

get_access_code({UserGID, AccessToken}) ->
    try
        Uri = "/openid/client/access_code",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        AccessCode = proplists:get_value(<<"accessCode">>, mochijson2:decode(ResBody, [{format, proplist}])),
        true = (AccessCode =/= undefiend),
        {ok, AccessCode}
    catch
        _:Reason ->
            ?error("Cannot get access code: ~p", [Reason]),
            {error, Reason}
    end.

get_tokens({UserGID, AccessToken}) ->
    try
        Uri = "/openid/client/tokens",
        {ok, "200", _ResHeaders, ResBody} = send_req(Uri, get, {UserGID, AccessToken}),
        TokenInfo = proplists:get_value(<<"tokenInfo">>, mochijson2:decode(ResBody, [{format, proplist}])),
        Tokens = lists:map(fun(Token) ->
            {
                proplists:get_value(<<"accessId">>, Token),
                proplists:get_value(<<"clientName">>, Token)
            }
        end, TokenInfo),
        {ok, Tokens}
    catch
        _:Reason ->
            ?error("Cannot get access code: ~p", [Reason]),
            {error, Reason}
    end.

delete_token(AccessId, {UserGID, AccessToken}) ->
    try
        Uri = "/openid/client/tokens/" ++ binary_to_list(AccessId),
        {ok, "204", _ResHeaders, _ResBody} = send_req(Uri, delete, {UserGID, AccessToken}),
        ok
    catch
        _:Reason ->
            ?error("Cannot get delete token: ~p", [Reason]),
            {error, Reason}
    end.