%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page displays information about the user and allows some editing.
%% @end
%% ===================================================================

-module(page_manage_account).
-include("veil_modules/control_panel/common.hrl").

% n2o API
-export([main/0, event/1]).
% Postback functions
-export([api_event/3, update_email/2, update_dn/2, show_email_adding/1, show_dn_adding/1]).

%% Template points to the template file, which will be filled with content
main() ->
    case vcn_gui_utils:maybe_redirect(true, false, false, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.

%% Page title
title() -> <<"Manage account">>.

%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
    gui_jq:register_escape_event("escape_pressed"),
    #panel{style = <<"position: relative;">>, body = [
        vcn_gui_utils:top_menu(manage_account_tab),
        #panel{style = <<"margin-top: 60px; padding: 20px;">>, body = [
            #panel{id = <<"dn_error_panel">>, style = <<"display: none;">>,
                class = <<"dialog dialog-danger">>, body = [
                    #p{body = <<"To be able to use any functionalities, please do one of the following: ">>},
                    #panel{style = <<"margin-left: auto; margin-right: auto; text-align: left; display: inline-block;">>, body = [
                        #list{body = [
                            #li{style = <<"padding: 10px 0 0;">>,
                                body = <<"Enable certificate DN retrieval from your OpenID provider and log in again">>},
                            #li{style = <<"padding: 7px 0 0;">>,
                                body = <<"Add your .pem certificate manually below">>}
                        ]}
                    ]}
                ]},
            #panel{id = <<"unverified_dns_panel">>, style = <<"display: none;">>,
                class = <<"dialog dialog-danger">>, body = [
                    #p{body = <<"Some of certificates you added are not verified. To verify them, connect to the system using VeilClient ",
                    "with a matching private key.">>}
                ]},
            #panel{id = <<"helper_error_panel">>, style = <<"display: none;">>,
                class = <<"dialog dialog-danger">>, body = [
                    #p{body = <<"To be able to use any functionalities, there must be at least one storage helper defined.">>}
                ]},
            #h6{style = <<" text-align: center;">>, body = <<"Manage account">>},
            #panel{id = <<"main_table">>, body = main_table()}
        ]}
    ] ++ vcn_gui_utils:logotype_footer(20)}.


% Info to register a DN
maybe_display_dn_message() ->
    case user_logic:get_dn_list(gui_ctx:get_user_record()) of
        [] -> gui_jq:show(<<"dn_error_panel">>);
        _ -> gui_jq:hide(<<"dn_error_panel">>)
    end.


% Info to verify a DN
maybe_display_verify_dn_message() ->
    case user_logic:get_unverified_dn_list(gui_ctx:get_user_record()) of
        [] -> gui_jq:hide(<<"unverified_dns_panel">>);
        _ -> gui_jq:show(<<"unverified_dns_panel">>)
    end.


% Info to install a storage helper
maybe_display_helper_message() ->
    case vcn_gui_utils:storage_defined() of
        false -> gui_jq:show(<<"helper_error_panel">>);
        true -> gui_jq:hide(<<"helper_error_panel">>)
    end.


% Snippet generating account managment table
main_table() ->
    maybe_display_dn_message(),
    maybe_display_verify_dn_message(),
    maybe_display_helper_message(),
    User = gui_ctx:get_user_record(),
    #table{style = <<"border-width: 0px; width: auto;">>, body = [
        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Login">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = #p{body = user_logic:get_login(User)}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Name">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = #p{body = user_logic:get_name(User)}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Teams">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = team_list_body()}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"E-mails">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = email_list_body()}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Certificates&#8217 DNs">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = dn_list_body()}
        ]}
    ]}.


% HTML list with teams printed
team_list_body() ->
    User = gui_ctx:get_user_record(),
    Teams = user_logic:get_teams(User),
    _Body = case lists:map(
        fun(Team) ->
            #li{style = <<"font-size: 18px; padding: 5px 0;">>,
                body = gui_str:html_encode(re:replace(Team, "\\(", " (", [global, {return, list}]))}
        end, Teams) of
                [] -> #p{body = <<"none">>};
                List -> #list{numbered = true, body = List}
            end.


% HTML list with emails printed
email_list_body() ->
    User = gui_ctx:get_user_record(),
    {CurrentEmails, _} = lists:mapfoldl(
        fun(Email, Acc) ->
            Body = #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = #span{body =
            [
                gui_str:html_encode(Email),
                #link{id = <<"remove_email_button", (integer_to_binary(Acc))/binary>>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                    postback = {action, update_email, [User, {remove, Email}]}, body =
                    #span{class = <<"fui-cross">>, style = <<"font-size: 16px;">>}}
            ]}},
            {Body, Acc + 1}
        end, 1, user_logic:get_email_list(User)),
    NewEmail = [
        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
            #link{id = <<"add_email_button">>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                postback = {action, show_email_adding, [true]}, body =
                #span{class = <<"fui-plus">>, style = <<"font-size: 16px; position: relative;">>}},
            #textbox{id = <<"new_email_textbox">>, class = <<"flat">>, body = <<"">>, style = <<"display: none;">>,
                placeholder = <<"New email address">>},
            #link{id = <<"new_email_submit">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                actions = gui_jq:form_submit_action(<<"new_email_submit">>, {action, update_email, [User, {add, submitted}]}, <<"new_email_textbox">>),
                body = #span{class = <<"fui-check-inverted">>, style = <<"font-size: 20px;">>}},
            #link{id = <<"new_email_cancel">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, show_email_adding, [false]}, body =
                #span{class = <<"fui-cross-inverted">>, style = <<"font-size: 20px;">>}}
        ]}
    ],
    gui_jq:bind_enter_to_submit_button(<<"new_email_textbox">>, <<"new_email_submit">>),
    #list{numbered = true, body = CurrentEmails ++ NewEmail}.


% HTML list with DNs printed
dn_list_body() ->
    User = gui_ctx:get_user_record(),
    {CurrentDNs, Counter} = lists:mapfoldl(
        fun(DN, Acc) ->
            Body = #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = #span{body =
            [
                gui_str:html_encode(DN),
                #link{id = <<"remove_dn_button", (integer_to_binary(Acc))/binary>>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                    postback = {action, update_dn, [User, {remove, DN}]}, body =
                    #span{class = <<"fui-cross">>, style = <<"font-size: 16px;">>}}
            ]}},
            {Body, Acc + 1}
        end, 1, user_logic:get_dn_list(User)),
    {UnverifiedDNs, _} = lists:mapfoldl(
        fun(DN, Acc) ->
            Body = #li{style = <<"font-size: 18px; padding: 5px 0; color: #90A5C0;">>, body = #span{body =
            [
                gui_str:html_encode(DN),
                <<"<i style=\"color: #ff6363\">&nbsp;&nbsp;(unverified)</i>">>,
                #link{id = <<"remove_unverified_dn_button", (integer_to_binary(Acc))/binary>>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                    postback = {action, update_dn, [User, {remove_unverified, DN}]}, body =
                    #span{class = <<"fui-cross">>, style = <<"font-size: 16px;">>}}
            ]}},
            {Body, Acc + 1}
        end, Counter, user_logic:get_unverified_dn_list(User)),
    NewDN = [
        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
            #link{id = <<"add_dn_button">>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                postback = {action, show_dn_adding, [true]}, body =
                #span{class = <<"fui-plus">>, style = <<"font-size: 16px;">>}},
            #textarea{id = <<"new_dn_textbox">>, style = <<"display: none; font-size: 12px; width: 600px; height: 200px;",
            "vertical-align: top; overflow-y: scroll;">>, body = <<"">>, placeholder = <<"Paste your .pem public certificate here...">>},
            #link{id = <<"new_dn_submit">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                actions = gui_jq:form_submit_action(<<"new_dn_submit">>, {action, update_dn, [User, {add, submitted}]}, <<"new_dn_textbox">>),
                body = #span{class = <<"fui-check-inverted">>, style = <<"font-size: 20px;">>}},
            #link{id = <<"new_dn_cancel">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, show_dn_adding, [false]}, body =
                #span{class = <<"fui-cross-inverted">>, style = <<"font-size: 20px;">>}}
        ]}
    ],
    #list{numbered = true, body = CurrentDNs ++ UnverifiedDNs ++ NewDN}.


% Postback event handling
api_event("escape_pressed", _, _) ->
    show_email_adding(false),
    show_dn_adding(false).

event(init) -> ok;

event({action, Fun}) ->
    event({action, Fun, []});

event({action, Fun, Args}) ->
    vcn_gui_utils:apply_or_redirect(?MODULE, Fun, Args, false);

event(terminate) -> ok.


% Update email list - add or remove one and save new user doc
update_email(User, AddOrRemove) ->
    OldEmailList = user_logic:get_email_list(User),
    {ok, NewUser} = case AddOrRemove of
                        {add, submitted} ->
                            NewEmail = gui_str:to_list(gui_ctx:form_param(<<"new_email_textbox">>)),
                            case user_logic:get_user({email, NewEmail}) of
                                {ok, _} ->
                                    gui_jq:wire(#alert{text = <<"This e-mail address is in use.">>}),
                                    {ok, User};
                                _ ->
                                    user_logic:update_email_list(User, OldEmailList ++ [NewEmail])
                            end;
                        {remove, Email} ->
                            user_logic:update_email_list(User, OldEmailList -- [Email])
                    end,
    gui_ctx:set_user_record(NewUser),
    gui_jq:update(<<"main_table">>, main_table()).


% Update DN list - add or remove one and save new user doc
update_dn(User, AddOrRemove) ->
    OldUnvDnList = user_logic:get_unverified_dn_list(User),
    case AddOrRemove of
        {add, submitted} ->
            case user_logic:extract_dn_from_cert(gui_ctx:form_param(<<"new_dn_textbox">>)) of
                {rdnSequence, RDNSequence} ->
                    {ok, DnString} = user_logic:rdn_sequence_to_dn_string(RDNSequence),
                    case user_logic:get_user({dn, DnString}) of
                        {ok, _} ->
                            gui_jq:wire(#alert{text = <<"This certificate is already registered.">>});
                        _ ->
                            case user_logic:get_user({unverified_dn, DnString}) of
                                {ok, _} ->
                                    gui_jq:wire(#alert{text = <<"This certificate is already registered.">>});
                                _ ->
                                    {ok, NewUser} = user_logic:update_unverified_dn_list(User, OldUnvDnList ++ [DnString]),
                                    gui_ctx:set_user_record(NewUser)
                            end
                    end;
                {error, proxy_ceertificate} ->
                    gui_jq:wire(#alert{text = <<"Proxy certificates are not accepted.">>});
                {error, self_signed} ->
                    gui_jq:wire(#alert{text = <<"Self signed certificates are not accepted.">>});
                {error, extraction_failed} ->
                    gui_jq:wire(#alert{text = <<"Unable to process certificate.">>})
            end;
        {remove, DN} ->
            OldDnList = user_logic:get_dn_list(User),
            {ok, NewUser} = user_logic:update_dn_list(User, OldDnList -- [DN]),
            gui_ctx:set_user_record(NewUser);
        {remove_unverified, DN} ->
            {ok, NewUser} = user_logic:update_unverified_dn_list(User, OldUnvDnList -- [DN]),
            gui_ctx:set_user_record(NewUser)
    end,
    gui_jq:update(<<"main_table">>, main_table()).


% Show email adding form
show_email_adding(Flag) ->
    case Flag of
        true ->
            gui_jq:hide(<<"add_email_button">>),
            gui_jq:fade_in(<<"new_email_textbox">>, 300),
            gui_jq:fade_in(<<"new_email_cancel">>, 300),
            gui_jq:fade_in(<<"new_email_submit">>, 300),
            gui_jq:focus(<<"new_email_textbox">>);
        false ->
            gui_jq:fade_in(<<"add_email_button">>, 300),
            gui_jq:hide(<<"new_email_textbox">>),
            gui_jq:hide(<<"new_email_cancel">>),
            gui_jq:hide(<<"new_email_submit">>)
    end.


% Show DN adding form
show_dn_adding(Flag) ->
    case Flag of
        true ->
            gui_jq:hide(<<"add_dn_button">>),
            gui_jq:fade_in(<<"new_dn_textbox">>, 300),
            gui_jq:fade_in(<<"new_dn_cancel">>, 300),
            gui_jq:fade_in(<<"new_dn_submit">>, 300),
            gui_jq:focus(<<"new_dn_textbox">>);
        false ->
            gui_jq:fade_in(<<"add_dn_button">>, 300),
            gui_jq:hide(<<"new_dn_textbox">>),
            gui_jq:hide(<<"new_dn_cancel">>),
            gui_jq:hide(<<"new_dn_submit">>)
    end.
