%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page contains information about the project, licence and contact for support.
%% @end
%% ===================================================================

-module(page_about).
-include("oneprovider_modules/control_panel/common.hrl").
-include("registered_names.hrl").

% n2o API
-export([main/0, event/1]).

-define(LICENSE_FILE, "LICENSE.txt").
-define(CONTACT_EMAIL, "support@onedata.org").

%% Template points to the template file, which will be filled with content
main() ->
    case opn_gui_utils:maybe_redirect(true, false) of
        true ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.

%% Page title
title() -> <<"About">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    #panel{class = <<"page-container">>, body = [
        opn_gui_utils:top_menu(about_tab),
        #panel{style = <<"margin-top: 60px; padding: 20px;">>, body = [
            #h6{style = <<" text-align: center;">>, body = <<"About">>},
            #panel{id = <<"about_table">>, body = about_table()}
        ]}
    ]}.

about_table() ->
    #table{style = <<"border-width: 0px; width: auto">>,
        body = #tbody{body = [
            #tr{cells = [
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Version">>}},
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #p{style = <<"margin: -3px 0 0;">>, body = node_manager:check_vsn()}}
            ]},

            #tr{cells = [
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Contact">>}},
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #link{style = <<"font-size: 18px;">>, body = <<?CONTACT_EMAIL>>, url = <<"mailto:", ?CONTACT_EMAIL>>}}
            ]},

            #tr{cells = [
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Privacy policy">>}},
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #link{style = <<"font-size: 18px; padding: 5px 0;">>, body = <<"Learn about privacy policy">>, url = <<?privacy_policy_url>>}}
            ]},

            #tr{cells = [
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Acknowledgements">>}},
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #p{style = <<"margin: -3px 0 0;">>, body = <<"This research was supported in part by PL-Grid Infrastructure.">>}}
            ]},

            #tr{cells = [
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"License">>}},
                #td{style = <<"padding: 15px; vertical-align: top;">>,
                    body = #p{style = <<"white-space: pre; font-size: 100%; line-height: normal">>, body = get_license()}}
            ]}
%%
%%         #tr{cells = [
%%             #td{style = <<"padding: 15px; vertical-align: top;">>,
%%                 body = #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Team">>}},
%%             #td{style = <<"padding: 15px; vertical-align: top;">>, body = get_team()}
%%         ]}
        ]}
    }.

% content of LICENSE.txt file
get_license() ->
    case file:read_file(?LICENSE_FILE) of
        {ok, File} -> File;
        {error, _Error} -> <<"">>
    end.

%% % HTML list with team members
%% get_team() ->
%%     Members = [<<"Łukasz Dutka"/utf8>>, <<"Jacek Kitowski"/utf8>>, <<"Dariusz Król"/utf8>>, <<"Tomasz Lichoń"/utf8>>, <<"Darin Nikolow"/utf8>>,
%%         <<"Łukasz Opioła"/utf8>>, <<"Tomasz Pałys"/utf8>>, <<"Bartosz Polnik"/utf8>>, <<"Paweł Salata"/utf8>>, <<"Michał Sitko"/utf8>>,
%%         <<"Rafał Słota"/utf8>>, <<"Renata Słota"/utf8>>, <<"Beata Skiba"/utf8>>, <<"Krzysztof Trzepla"/utf8>>, <<"Michał Wrzeszcz"/utf8>>,
%%         <<"Konrad Zemek"/utf8>>],
%%     #list{style = <<"margin-top: -3px;">>, numbered = false, body =
%%     lists:map(
%%         fun(Member) ->
%%             #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = Member}
%%         end, Members)
%%     }.

event(init) -> ok;
event(terminate) -> ok.
