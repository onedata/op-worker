%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager's data distribution view component.
%% It consists of several functions that render n2o elements and implement logic.
%% @end
%% ===================================================================
-module(pfm_data_dist).

-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, data_distribution_panel/2, comet_loop_init/2, on_resize_js/0]).
-export([toggle_ddist_view/3, sync_file/2, expel_file/2]).

% Macros used to generate IDs of certain elements
-define(DIST_PANEL_ID(RowID), <<"dd_", (integer_to_binary(RowID))/binary>>).
-define(SHOW_DIST_PANEL_ID(RowID), <<"show_dd_", (integer_to_binary(RowID))/binary>>).
-define(HIDE_DIST_PANEL_ID(RowID), <<"hide_dd_", (integer_to_binary(RowID))/binary>>).
-define(CANVAS_ID(RowID, ProviderID), <<"canvas_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).
-define(SYNC_BUTTON_ID(RowID, ProviderID), <<"sync_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).
-define(EXPEL_BUTTON_ID(RowID, ProviderID), <<"expel_", (integer_to_binary(RowID))/binary, "_", ProviderID/binary>>).

% Reference to comet pid that handles updates of data distribution view
-define(DD_COMET_PID, dd_comet_pid).


%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Initializes comet process that handles async updates of file distribution status.
%% @end
-spec init() -> term().
%% ====================================================================
init(GRUID, AccessToken, MainCometPid) ->
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken) end),
    % Store DDCometPid in file manager's main comet process's memory
    MainCometPid ! {put, ?DD_COMET_PID, Pid}.


%% data_distribution_panel/2
%% ====================================================================
%% @doc Renders data distribution panel to be concatenated to every file row in list view.
%% @end
-spec data_distribution_panel(FullPath :: binary(), RowID :: integer()) -> [term()].
%% ====================================================================
data_distribution_panel(FullPath, RowID) ->
    ShowDDistID = ?SHOW_DIST_PANEL_ID(RowID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(ShowDDistID, <<"function(e) { e.stopPropagation(); }">>),
    HideDDistID = ?HIDE_DIST_PANEL_ID(RowID),
    % Item won't hightlight if the link is clicked.
    gui_jq:bind_element_click(HideDDistID, <<"function(e) { e.stopPropagation(); }">>),
    DDistPanelID = ?DIST_PANEL_ID(RowID),
    [
        #link{id = ShowDDistID, postback = {action, ?MODULE, toggle_ddist_view, [FullPath, RowID, true]},
            title = <<"Data distribution (advanced)">>, class = <<"glyph-link hidden show-on-parent-hover ddist-show-button">>,
            body = #span{class = <<"icomoon-earth">>}},
        #link{id = HideDDistID, postback = {action, ?MODULE, toggle_ddist_view, [FullPath, RowID, false]},
            title = <<"Hide data distribution view">>, class = <<"glyph-link hidden ddist-hide-button">>,
            body = #span{class = <<"icomoon-minus4">>}},
        #panel{class = <<"clearfix">>},
        #panel{id = DDistPanelID, class = <<"ddist-panel">>, body = <<"">>}
    ].


%% toggle_ddist_view/3
%% ====================================================================
%% @doc Function evaluated in postback that shows or hides the data distribution panel.
%% @end
-spec toggle_ddist_view(FullPath :: binary(), RowID :: integer(), Flag :: boolean()) -> term().
%% ====================================================================
toggle_ddist_view(FullPath, RowID, Flag) ->
    ShowDDistID = ?SHOW_DIST_PANEL_ID(RowID),
    HideDDistID = ?HIDE_DIST_PANEL_ID(RowID),
    DDistPanelID = ?DIST_PANEL_ID(RowID),
    case Flag of
        true ->
            gui_jq:update(DDistPanelID, render_table(FullPath, RowID)),
            gui_jq:add_class(HideDDistID, <<"show-on-parent-hover">>),
            gui_jq:remove_class(ShowDDistID, <<"show-on-parent-hover">>),
            gui_jq:slide_down(DDistPanelID, 400);
        false ->
            gui_jq:add_class(ShowDDistID, <<"show-on-parent-hover">>),
            gui_jq:remove_class(HideDDistID, <<"show-on-parent-hover">>),
            gui_jq:slide_up(DDistPanelID, 200)
    end,
    get(?DD_COMET_PID) ! {toggle_watching, FullPath, RowID, Flag}.


%% refresh_view/2
%% ====================================================================
%% @doc Refreshes the distribution status of given file.
%% @end
-spec refresh_view(FullPath :: binary(), RowID :: integer()) -> term().
%% ====================================================================
refresh_view(FullPath, RowID) ->
    gui_jq:update(?DIST_PANEL_ID(RowID), render_table(FullPath, RowID)).


%% render_table/2
%% ====================================================================
%% @doc Renders the table with distribution status for given file.
%% @end
-spec render_table(FullPath :: binary(), RowID :: integer()) -> term().
%% ====================================================================
render_table(FilePath, RowID) ->
    {FileSize, Blocks} = fs_interface:file_parts_mock(FilePath),
    gui_jq:wire("$(window).resize();"),
    #table{class = <<"ddist-table">>,
        body = #tbody{body = [
            lists:map(
                fun({ProviderID, ProvBytes, BlockList}) ->
                    CanvasID = ?CANVAS_ID(RowID, ProviderID),
                    SyncButtonID = ?SYNC_BUTTON_ID(RowID, ProviderID),
                    ExpelButtonID = ?EXPEL_BUTTON_ID(RowID, ProviderID),
                    JSON = rest_utils:encode_to_json([{<<"file_size">>, FileSize}, {<<"chunks">>, BlockList}]),
                    gui_jq:wire(<<"new FileChunksBar(document.getElementById('", CanvasID/binary, "'), '", JSON/binary, "');">>),
                    #tr{cells = [
                        #td{body = ProviderID, class = <<"ddist-provider">>},
                        #td{body = gui_str:format_bin("~.2f%", [ProvBytes * 100 / FileSize]), class = <<"ddist-percentage">>},
                        #td{body = #canvas{id = CanvasID, class = <<"ddist-canvas">>}},
                        #td{body = #link{id = SyncButtonID, postback = {action, ?MODULE, sync_file, [FilePath, ProviderID]},
                            title = <<"Issue full synchronization">>, class = <<"glyph-link ddist-button">>,
                            body = #span{class = <<"icomoon-spinner6">>}}},
                        #td{body = #link{id = ExpelButtonID, postback = {action, ?MODULE, expel_file, [FilePath, ProviderID]},
                            title = <<"Expel all chunks from this provider">>, class = <<"glyph-link ddist-button">>,
                            body = #span{class = <<"icomoon-blocked">>}}}
                    ]}
                end, Blocks)
        ]}
    }.


%% sync_file/2
%% ====================================================================
%% @doc Issues full synchronization (downloading all the blocks) of selected file.
%% @end
-spec sync_file(FullPath :: binary(), ProviderID :: string()) -> term().
%% ====================================================================
sync_file(FilePath, ProviderID) ->
    ?dump(sync_file).


%% expel_file/2
%% ====================================================================
%% @doc Issues removal of all file blocks from selected provider.
%% @end
-spec expel_file(FullPath :: binary(), ProviderID :: string()) -> term().
%% ====================================================================
expel_file(FilePath, ProviderID) ->
    ?dump(expel_file).


%% on_resize_js/0
%% ====================================================================
%% @doc JavaScript snippet that handles scaling of data distibution panel.
%% @end
-spec on_resize_js() -> binary().
%% ====================================================================
on_resize_js() ->
    <<"if ($($('.list-view-name-header')[0]).width() < 400) { ",
    "$('.ddist-percentage').hide(); $('.ddist-canvas').css('width', '80px'); } else ",
    "{ $('.ddist-percentage').show(); $('.ddist-canvas').css('width', '125px'); }">>.


%% comet_loop_init/2
%% ====================================================================
%% @doc Function evaluated by comet process to initialize its state.
%% @end
-spec comet_loop_init(GRUID :: binary(), UserAccessToken :: binary()) -> term().
%% ====================================================================
comet_loop_init(GRUID, UserAccessToken) ->
    % Initialize context
    fslogic_context:set_gr_auth(GRUID, UserAccessToken),
    gui_comet:flush(),
    comet_loop([]).


%% comet_loop/1
%% ====================================================================
%% @doc Main comet loop that handles async updates of files distribution status.
%% @end
-spec comet_loop(WatchedFiles :: [term()]) -> term().
%% ====================================================================
comet_loop(WatchedFiles) ->
    NewWatchedFiles = receive
                          {toggle_watching, FullPath, RowID, Flag} ->
                              case Flag of
                                  true ->
                                      [{FullPath, RowID} | WatchedFiles];
                                  false ->
                                      WatchedFiles -- [{FullPath, RowID}]
                              end
                      after
                          50000 ->
                              % TODO mock
                              lists:foreach(
                                  fun({FullPath, RowID}) ->
                                      refresh_view(FullPath, RowID)
                                  end, WatchedFiles),
                              gui_comet:flush(),
                              WatchedFiles
                      end,
    comet_loop(NewWatchedFiles).
