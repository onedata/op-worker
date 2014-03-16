%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains Nitrogen website code
%% @end
%% ===================================================================

-module(page_about).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").

-define(path_to_license_file, "../../../LICENSE.txt").
-define(contact_email, "support@onedata.org").

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}.

%% Page title
title() -> <<"About">>.

%% This will be placed in the template instead of {{body}} tag
body() -> gui_utils:apply_or_redirect(?MODULE, render_body, true).

render_body() ->
    #panel{style = <<"position: relative;">>, body = [
        gui_utils:top_menu(about_tab),
        #panel{style = <<"margin-top: 60px; padding: 20px;">>, body = [
            #panel{id = "about_table", body = about_table()}
        ]}
    ] ++ gui_utils:logotype_footer(20)}.

about_table() ->
    #table{style = <<"border-width: 0px; width: auto">>, body = [
        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Version">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = #p{body = node_manager:check_vsn()}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Contact">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #link{style = <<"font-size: 18px; padding: 5px 0;">>, body = <<?contact_email>>, url = <<"mailto:", ?contact_email>>}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Acknowledgements">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #p{body = <<"This research was supported in part by PL-Grid Infrastructure.">>}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"License">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>,
            body = #p{style = <<"white-space: pre; font-size: 100%; line-height: normal">>, body = get_license()}}
        ]},

        #tr{cells = [
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body =
            #label{class = <<"label label-large label-inverse">>, style = <<"cursor: auto;">>, body = <<"Developers">>}},
            #td{style = <<"border-width: 0px; padding: 10px 10px">>, body = get_developers()}
        ]}

    ]}.

% content of LICENSE.txt file
get_license() ->
    case file:read_file(?path_to_license_file) of
        {ok, File} -> File;
        {error, Error} -> wf:to_binary(Error)
    end.

% HTML list with developers printed
get_developers() ->
    Developers = [<<"Łukasz Dutka">>, <<"Jacek Kitowski">>, <<"Dariusz Król">>, <<"Tomasz Lichoń">>, <<"Darin Nikolow">>,
        <<"Łukasz Opioła">>, <<"Tomasz Pałys">>, <<"Bartosz Polnik">>, <<"Paweł Salata">>, <<"Michał Sitko">>,
        <<"Rafał Słota">>, <<"Renata Słota">>, <<"Beata Skiba">>, <<"Krzysztof Trzepla">>, <<"Michał Wrzeszcz">>],
    #list{numbered = false, body =
    lists:map(
        fun(Developer) ->
            #li{style = <<"font-size: 18px; padding: 5px 0;">>, body = Developer}
        end, Developers)
    }.

event(init) -> ok.