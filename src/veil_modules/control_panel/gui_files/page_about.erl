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

main() -> #template{file = "./gui_static/templates/bare.html"}.

title() -> "About".

body() ->
  gui_utils:apply_or_redirect(?MODULE, render_body, true).

render_body() ->
  #panel{style = "position: relative;", body = [
    gui_utils:top_menu(about_tab),
    #panel{style = "margin-top: 60px; padding: 20px;", body = [
      #panel{id = about_table, body = about_table()}
    ]}
  ] ++ gui_utils:logotype_footer(20)}.

about_table() ->
  #table{style = "border-width: 0px; width: auto", rows = [

    #tablerow{cells = [
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #label{class = "label label-large label-inverse", style = "cursor: auto;", text = "Version"}},
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body = #p{text = node_manager:check_vsn()}}
    ]},

    #tablerow{cells = [
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #label{class = "label label-large label-inverse", style = "cursor: auto;", text = "Contact"}},
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #email_link { style="font-size: 18px; padding: 5px 0;", email=?contact_email }}
    ]},

    #tablerow{cells = [
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #label{class = "label label-large label-inverse", style = "cursor: auto;", text = "Acknowledgements"}},
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #p{text = "This research was supported in part by PL-Grid Infrastructure."}}
    ]},

    #tablerow{cells = [
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #label{class = "label label-large label-inverse", style = "cursor: auto;", text = "License"}},
      #tablecell{style = "border-width: 0px; padding: 10px 10px",
      body = #p{style = "white-space: pre; font-size: 100%; line-height: normal", text = get_license()}}
    ]},

    #tablerow{cells = [
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body =
      #label{class = "label label-large label-inverse", style = "cursor: auto;", text = "Developers"}},
      #tablecell{style = "border-width: 0px; padding: 10px 10px", body = get_developers()}
    ]}

  ]}.

% content of LICENSE.txt file
get_license() ->
  case file:read_file(?path_to_license_file) of
    {ok, File} -> binary:bin_to_list(File);
    {error, Error} -> Error
  end.

% HTML list with developers printed
get_developers() ->
  Developers = ["Łukasz Dutka", "Jacek Kitowski", "Dariusz Król", "Tomasz Lichoń", "Darin Nikolow",
    "Łukasz Opioła", "Tomasz Pałys", "Bartosz Polnik", "Paweł Salata", "Michał Sitko",
    "Rafał Słota", "Renata Słota", "Beata Skiba", "Krzysztof Trzepla", "Michał Wrzeszcz"],
  #list { numbered=false, body =
  lists:map(
    fun(Developer) ->
      #listitem { style="font-size: 18px; padding: 5px 0;", body=Developer }
    end, Developers)
  }.
