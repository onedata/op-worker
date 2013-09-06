%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains nitrogen website code
%% @end
%% ===================================================================

-module (index).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").
-include("registered_names.hrl").

%% Template points to the template file, which will be filled with content
main() ->
  #template { file="./gui_static/templates/logs.html" }.

%% Page title
title() -> "VeilFS - log stream".

%% This will be placed in the template instead of [[[page:header()]]] tag
header() ->
  #panel
  {
    class = header,
    body = 
    [
      #link { class = header_link, text="MAIN PAGE", url="/index" },
      #link { class = header_link, text="LOGIN / LOGOUT", url="/login" },
      #link { class = header_link, text="MANAGE ACCOUNT", url="/manage_account" }
    ]
  }.


%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
  % start of a comet process
  {ok, Pid} = wf:comet(fun() -> comet_loop_init() end),
  % the process subscribes for logs at central_logger
  gen_server:call(?Dispatcher_Name, {central_logger, 1, {subscribe, Pid}}),
  [
    #panel { class = logs, body = 
    [
      #h2 { text = "LOGS" },
      #panel { id = logs }
    ]}
  ].

% initialization of comet loop - trap_exit=true so we can control when a session terminates and
% the process should be removed from central_logger subscribers
comet_loop_init() ->
  process_flag(trap_exit, true),
  comet_loop(1).

% comet loop - waits for new logs, updates the page and repeats
comet_loop(Counter) ->
  try
    receive
      {log, Log} ->
        wf:insert_bottom(logs, format_log(Log, Counter)),
        wf:wire(list_to_atom("expanded_log_" ++ integer_to_list(Counter)), #hide {}),
        wf:flush(),
        comet_loop(Counter + 1);
      {'EXIT', _, _Reason} ->
        gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, self()}});
      _ ->
        comet_loop(Counter)
    end
  catch
    Class:Term ->
      gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, self()}}),
      wf:insert_top(logs, io_lib:format("Error in comet process<br />~p : ~p<br />", [Class, Term])),
      wf:flush()
  end.

% returns a Nitrogen-HTML-formatted log to be added to the site
format_log({Message, Timestamp, Severity, Metadata}, Counter) ->
  [    
    #table { class = log, rows = 
    [
      #tablerow { cells = 
      [
        #tablecell 
        { 
          class = atom_to_list(log_header) ++ " severity_" ++ atom_to_list(Severity),
          body =  "===== " ++ string:to_upper(atom_to_list(Severity)) ++ " ====="
        },
        #tablecell 
        {
          class = atom_to_list(timestamp) ++ " severity_" ++ atom_to_list(Severity),
          body =  begin 
                    {_, _, Micros} = Timestamp,
                    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
                    io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w.~3..0w",
                                  [YY, MM, DD, Hour, Min, Sec, Micros div 1000])
                  end
        }
      ]},
      #tablerow 
      { 
        id = list_to_atom("collapsed_log_" ++ integer_to_list(Counter)),
        actions = #event{type = click, postback = {toggle_log, Counter, true}},
        cells = 
        [            
          #tablecell 
          {
            class = metadata,
            body = 
            [
              case lists:keyfind(node, 1, Metadata) of
                {Key, Value} ->
                  "<b>" ++ wf:to_list(Key) ++ ":</b> " ++ wf:to_list(Value) ++ " ...";
                _ ->
                  "<b>unknown node</b> ..."
              end
            ]
          },
          #tablecell 
          {
            class = metadata_hint,
            body = "click to expand"
          }
        ]
      },
      #tablerow 
      { 
        id = list_to_atom("expanded_log_" ++ integer_to_list(Counter)),
        actions = #event{type=click, postback = {toggle_log, Counter, false}},
        cells = 
        [            
          #tablecell 
          {
            class = metadata,
            body =  case Metadata of
                      [] ->   "unknown node";
                      Tags -> lists:foldl
                              (
                                fun({Key, Value}, Acc) ->
                                  Acc ++ "<b>" ++ to_list(Key) ++ ":</b> " ++ to_list(Value) ++ "<br />"
                                end,
                                [],
                                Tags
                              )
                      end
          },
          #tablecell 
          {
            class = metadata_hint,
            body = "click to collapse"
          }
        ]
      },
      #tablerow
      { cells = 
        [
          #tablecell 
          {
            colspan = 2,
            class = log_content,
            body = Message
          }
        ]
      }
    ]},
    #br{}
  ].

% event handling (collapsing and expanding logs)
event({toggle_log, Id, ShowAll}) ->
  case ShowAll of
    true -> 
      wf:wire(list_to_atom("expanded_log_" ++ integer_to_list(Id)), #appear { speed=500 }),
      wf:wire(list_to_atom("collapsed_log_" ++ integer_to_list(Id)), #hide{ });
    false ->
      wf:wire(list_to_atom("expanded_log_" ++ integer_to_list(Id)), #hide{ }),
      wf:wire(list_to_atom("collapsed_log_" ++ integer_to_list(Id)), #appear { speed=500 })
  end,
  wf:flush().

% convenience function
to_list(Arg) when is_list(Arg) -> Arg;
to_list(Arg) ->
  try wf:to_list(Arg) of
    Res -> Res
  catch
    _:_ -> io_lib:format("~p", [Arg])
  end.