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
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

%% Template points to the template file, which will be filled with content
main() ->
    case gui_utils:maybe_redirect(true, false, false, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.

%% Page title
title() -> <<"Manage account">>.

%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
    #panel{style = <<"position: relative;">>, body = [
        gui_utils:top_menu(manage_account_tab),
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
            #panel{id = <<"helper_error_panel">>, style = <<"display: none;">>,
                class = <<"dialog dialog-danger">>, body = [
                    #p{body = <<"To be able to use any functionalities, there must be at least one storage helper defined.">>}
                ]},
            #h6{style = <<" text-align: center;">>, body = <<"Manage account">>},
            #panel{id = <<"main_table">>, body = main_table()}
        ]}
    ] ++ gui_utils:logotype_footer(20)}.


% Info to register a DN
maybe_display_dn_message() ->
    case user_logic:get_dn_list(wf:session(user_doc)) of
        [] -> wf:wire(#jquery{target = "dn_error_panel", method = ["show"]});
        _ -> wf:wire(#jquery{target = "dn_error_panel", method = ["hide"]})
    end.


% Info to install a storage helper
maybe_display_helper_message() ->
    case gui_utils:storage_defined() of
        false -> wf:wire(#jquery{target = "helper_error_panel", method = ["show"]});
        true -> wf:wire(#jquery{target = "helper_error_panel", method = ["hide"]})
    end.


% Snippet generating account managment table
main_table() ->
    maybe_display_dn_message(),
    maybe_display_helper_message(),
    User = wf:session(user_doc),
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
    User = wf:session(user_doc),
    Teams = user_logic:get_teams(User),
    _Body = case lists:map(
        fun(Team) ->
            #li{style = <<"font-size: 18px; padding: 5px 0;">>,
                body = list_to_binary(re:replace(Team, "\\(", " (", [global, {return, list}]))}
        end, Teams) of
                [] -> #p{body = <<"none">>};
                List -> #list{numbered = true, body = List}
            end.


% HTML list with emails printed
email_list_body() ->
    User = wf:session(user_doc),
    {CurrentEmails, _} = lists:mapfoldl(
        fun(Email, Acc) ->
            Body = #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = #span{body =
            [
                Email,
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
                placeholder = <<"New email address">>, postback = {action, update_email, [User, {add, submitted}]},
                source = ["new_email_textbox"]},
            #link{id = <<"new_email_submit">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, update_email, [User, {add, submitted}]}, source = ["new_email_textbox"], body =
                #span{class = <<"fui-check-inverted">>, style = <<"font-size: 20px;">>}},
            #link{id = <<"new_email_cancel">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, show_email_adding, [false]}, body =
                #span{class = <<"fui-cross-inverted">>, style = <<"font-size: 20px;">>}}
        ]}
    ],
    #list{numbered = true, body = CurrentEmails ++ NewEmail}.


% HTML list with DNs printed
dn_list_body() ->
    User = wf:session(user_doc),
    {CurrentDNs, _} = lists:mapfoldl(
        fun(DN, Acc) ->
            Body = #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = #span{body =
            [
                DN,
                #link{id = <<"remove_dn_button", (integer_to_binary(Acc))/binary>>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                    postback = {action, update_dn, [User, {remove, DN}]}, body =
                    #span{class = <<"fui-cross">>, style = <<"font-size: 16px;">>}}
            ]}},
            {Body, Acc + 1}
        end, 1, user_logic:get_dn_list(User)),
    NewDN = [
        #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = [
            #link{id = <<"add_dn_button">>, class = <<"glyph-link">>, style = <<"margin-left: 10px;">>,
                postback = {action, show_dn_adding, [true]}, body =
                #span{class = <<"fui-plus">>, style = <<"font-size: 16px;">>}},
            #textarea{id = <<"new_dn_textbox">>, style = <<"display: none; font-size: 12px; width: 600px; height: 200px;",
            "vertical-align: top; overflow-y: scroll;">>, body = <<"">>,
                source = ["new_dn_textbox"], placeholder = <<"Paste your .pem certificate here...">>},
            #link{id = <<"new_dn_submit">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, update_dn, [User, {add, submitted}]}, source = ["new_dn_textbox"], body =
                #span{class = <<"fui-check-inverted">>, style = <<"font-size: 20px;">>}},
            #link{id = <<"new_dn_cancel">>, class = <<"glyph-link">>, style = <<"display: none; margin-left: 10px;">>,
                postback = {action, show_dn_adding, [false]}, body =
                #span{class = <<"fui-cross-inverted">>, style = <<"font-size: 20px;">>}}
        ]}
    ],
    #list{numbered = true, body = CurrentDNs ++ NewDN}.


% Postback event handling
event(init) -> ok;

event({action, Fun}) ->
    event({action, Fun, []});

event({action, Fun, Args}) ->
    gui_utils:apply_or_redirect(?MODULE, Fun, Args, false).


% Update email list - add or remove one and save new user doc
update_email(User, AddOrRemove) ->
    OldEmailList = user_logic:get_email_list(User),
    {ok, NewUser} = case AddOrRemove of
                        {add, submitted} ->
                            NewEmail = gui_utils:to_list(wf:q("new_email_textbox")),
                            case user_logic:get_user({email, NewEmail}) of
                                {ok, _} ->
                                    wf:wire(#alert{text = <<"This e-mail address is in use.">>}),
                                    {ok, User};
                                _ ->
                                    user_logic:update_email_list(User, OldEmailList ++ [NewEmail])
                            end;
                        {remove, Email} ->
                            user_logic:update_email_list(User, OldEmailList -- [Email])
                    end,
    wf:session(user_doc, NewUser),
    gui_utils:update("main_table", main_table()).


% Update DN list - add or remove one and save new user doc
update_dn(User, AddOrRemove) ->
    OldDnList = user_logic:get_dn_list(User),
    case AddOrRemove of
        {add, submitted} ->
            case user_logic:extract_dn_from_cert(list_to_binary(wf:q("new_dn_textbox"))) of
                {rdnSequence, RDNSequence} ->
                    {ok, DnString} = user_logic:rdn_sequence_to_dn_string(RDNSequence),
                    case user_logic:get_user({dn, DnString}) of
                        {ok, _} ->
                            wf:wire(#alert{text = <<"This certificate is already registered.">>});
                        _ ->
                            {ok, NewUser} = user_logic:update_dn_list(User, OldDnList ++ [DnString]),
                            wf:session(user_doc, NewUser)
                    end;
                {error, proxy_ceertificate} ->
                    wf:wire(#alert{text = <<"Proxy certificates are not accepted.">>});
                {error, self_signed} ->
                    wf:wire(#alert{text = <<"Self signed certificates are not accepted.">>});
                {error, extraction_failed} ->
                    wf:wire(#alert{text = <<"Unable to process certificate.">>})
            end;
        {remove, DN} ->
            {ok, NewUser} = user_logic:update_dn_list(User, OldDnList -- [DN]),
            wf:session(user_doc, NewUser)
    end,
    gui_utils:update("main_table", main_table()).


% Show email adding form
show_email_adding(Flag) ->
    case Flag of
        true ->
            wf:wire(#jquery{target = "add_email_button", method = ["hide"]}),
            wf:wire(#jquery{target = "new_email_textbox", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_email_cancel", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_email_submit", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_email_textbox", method = ["focus"]});
        false ->
            wf:wire(#jquery{target = "add_email_button", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_email_textbox", method = ["hide"]}),
            wf:wire(#jquery{target = "new_email_cancel", method = ["hide"]}),
            wf:wire(#jquery{target = "new_email_submit", method = ["hide"]})
    end.


% Show DN adding form
show_dn_adding(Flag) ->
    case Flag of
        true ->
            wf:wire(#jquery{target = "add_dn_button", method = ["hide"]}),
            wf:wire(#jquery{target = "new_dn_textbox", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_dn_submit", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_dn_cancel", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_dn_textbox", method = ["focus"]});
        false ->
            wf:wire(#jquery{target = "add_dn_button", method = ["fadeIn"], args = [300]}),
            wf:wire(#jquery{target = "new_dn_textbox", method = ["hide"]}),
            wf:wire(#jquery{target = "new_dn_cancel", method = ["hide"]}),
            wf:wire(#jquery{target = "new_dn_submit", method = ["hide"]})
    end.
