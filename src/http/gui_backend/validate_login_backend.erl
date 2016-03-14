%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements page_backend_behaviour and is called
%%% when validate_login page is visited - it contains the logic
%%% that validates a redirect for login from GR.
%%% THIS IS A PROTOTYPE AND AN EXAMPLE OF IMPLEMENTATION.
%%% @end
%%%-------------------------------------------------------------------
-module(validate_login_backend).
-author("Lukasz Opiola").
-behaviour(page_backend_behaviour).

-compile([export_all]).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

page_init() ->
    case g_session:is_logged_in() of
        true ->
            ok;
        false ->
            SrlzdMacaroon = g_ctx:get_url_param(<<"code">>),
            {ok, Macaroon} = macaroon:deserialize(SrlzdMacaroon),
            {ok, Auth = #auth{}} = gui_auth_manager:authenticate(Macaroon),
            {ok, #document{value = #identity{user_id = UserId} = Identity}} =
                identity:get_or_fetch(Auth),
            {ok, _} = g_session:log_in(UserId, [Identity, Auth]),
            % @Todo mock, create some dir structure if not exists
            SessionId = g_session:get_session_id(),
            {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
            #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
            {ok, DefaultSpaceName} = oz_users:get_default_space({user, {Mac, DMacs}}),
            case logical_file_manager:ls(
                SessionId, {path, <<"/spaces/", DefaultSpaceName/binary>>}, 0, 1000) of
                {ok, []} ->
                    CreateInDefSpace = fun(Type, SubPath) ->
                        P = <<"/spaces/", DefaultSpaceName/binary, "/", SubPath/binary>>,
                        case Type of
                            dir ->
                                logical_file_manager:mkdir(SessionId, P, 8#777);
                            file ->
                                logical_file_manager:create(SessionId, P, 8#777)
                        end
                    end,
                    CreateInDefSpace(dir, <<"katalog">>),
                    CreateInDefSpace(dir, <<"katalog/pod-katalog">>),
                    CreateInDefSpace(dir, <<"katalog/pod-katalog/pod-pod-katalog">>),
                    CreateInDefSpace(dir, <<"nadrzedny">>),
                    CreateInDefSpace(dir, <<"nadrzedny/podrzedny I">>),
                    CreateInDefSpace(dir, <<"nadrzedny/podrzedny II">>),
                    CreateInDefSpace(dir, <<"nadrzedny/podrzedny III">>),
                    CreateInDefSpace(file, <<"plik.txt">>),
                    CreateInDefSpace(file, <<"prezentacja.ppt">>),
                    CreateInDefSpace(file, <<"artykul.pdf">>),
                    CreateInDefSpace(file, <<"katalog/plik-k.txt">>),
                    CreateInDefSpace(file, <<"katalog/prezentacja-k.ppt">>),
                    CreateInDefSpace(file, <<"katalog/artykul-k.pdf">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/plik-k-pk.txt">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/prezentacja-k-pk.ppt">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/artykul-k-pk.pdf">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/pod-pod-katalog/plik-k-pk-ppk.txt">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/pod-pod-katalog/prezentacja-k-pk-ppk.ppt">>),
                    CreateInDefSpace(file, <<"katalog/pod-katalog/pod-pod-katalog/artykul-k-pk-ppk.pdf">>),
                    CreateInDefSpace(file, <<"nadrzedny/plik-n.txt">>),
                    CreateInDefSpace(file, <<"nadrzedny/prezentacja-n.ppt">>),
                    CreateInDefSpace(file, <<"nadrzedny/artykul-n.pdf">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny I/plik-n-p1.txt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny I/prezentacja-n-p1.ppt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny I/artykul-n-p1.pdf">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny II/plik-n-p2.txt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny II/prezentacja-n-p2.ppt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny II/artykul-n-p2.pdf">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny III/plik-n-p3.txt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny III/prezentacja-n-p3.ppt">>),
                    CreateInDefSpace(file, <<"nadrzedny/podrzedny III/artykul-n-p3.pdf">>),
                    ok;
                _ ->
                    ok
            end
    end,
    {redirect_relative, <<"/">>}.
