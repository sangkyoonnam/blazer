module Blazer
  class BaseController < ApplicationController
    # skip filters

    filters = _process_action_callbacks.map(&:filter) - [:activate_authlogic]
    if Rails::VERSION::MAJOR >= 5
      skip_before_action(*filters, raise: false)
      skip_after_action(*filters, raise: false)
      skip_around_action(*filters, raise: false)
    else
      skip_action_callback *filters
    end

    protect_from_forgery with: :exception
    before_action :load_service

    if ENV["BLAZER_PASSWORD"]
      http_basic_authenticate_with name: ENV["BLAZER_USERNAME"], password: ENV["BLAZER_PASSWORD"]
    end

    if Blazer.before_action
      before_action Blazer.before_action.to_sym
    end

    layout "blazer/application"

    private

    def process_vars(statement, data_source)
      (@bind_vars ||= []).concat(Blazer.extract_vars(statement)).uniq!
      awesome_variables = {}
      @bind_vars = @bind_vars.reject{|var| var.end_with? '_table'} # 동적변수
      @bind_vars.each do |var|
        params[var] ||= Blazer.data_sources[data_source].variable_defaults[var]  # 현재 우리쪽에서는 쓰지않음
        awesome_variables[var] ||= Blazer.data_sources[data_source].awesome_variables[var]
      end
      @success = @bind_vars.all? { |v| params[v] } # parameter 로 각 동적변수들이 넘어왔는지 체크. 맨처음 페이지에 진입할때는 필요없다 여기서 아웃
      if @success
        @bind_vars.each do |var|       #bind_vars 변수와 param으로 넘어온 값들을 처리한다.
          value = params[var].presence
          if value
            if ["start_time", "end_time"].include?(var)
              value = value.to_s.gsub(" ", "+") # fix for Quip bug
            end

            if var.end_with?("_at")
              begin
                value = Blazer.time_zone.parse(value)
              rescue
                # do nothing
              end
            end

            if value =~ /\A\d+\z/
              value = value.to_i
            elsif value =~ /\A\d+\.\d+\z/
              value = value.to_f
            end

          end

          variable = awesome_variables[var]
          if variable.present? && variable['type'] == 'condition'
            if value.present? && variable['style'] == 'checkbox'
              statement.gsub!("{#{var}}"," #{value.join(' or ')} ")
            elsif value.present? && variable['style'] == 'file'
              table_name = "wheelhouse_temp.#{value}"
              statement.gsub!("{#{var}}", table_name)
            elsif value.present? || variable['style'] == 'text'
              statement.gsub!("{#{var}}", value.to_s)
            else
              statement.gsub!("{#{var}}", 'true')
            end
          else
            statement.gsub!("{#{var}}", ActiveRecord::Base.connection.quote(value))   #blazer.yml에 정의되어 있지 않는 변수에 대해서는 value값으로 치환해서 처리한다.
          end
        end
      end
    end

    #gcs 파일 링크로 빅쿼리에 적재해서 사용한다.
    def process_file_link(statement, data_source)
      awesome_variables = {}
      @bind_links = @bind_links.select{|var| var.start_with? 'gcs_file_link_'}
      return [] unless @bind_links.present?

      @bind_links.each do |var|
        params[var] ||= Blazer.data_sources[data_source].variable_defaults[var]
        awesome_variables[var] ||= Blazer.data_sources[data_source].awesome_variables[var]
      end

      @success = @bind_links.all? { |v| params[v] }
      if @success
        @bind_links.each do |var|
          awesome_variables[var] ||= Blazer.data_sources[data_source].awesome_variables[var]
        end

        @bind_links.each do |var|
          variable = awesome_variables[var]
          value = variable['value'][0]['table_name']
          statement.gsub!("{#{var}}", value )
        end
      end

    end

    def process_tables(statement, data_source)
      (@bind_tables ||= []).concat(Blazer.extract_vars(statement))
      awesome_variables = {}
      @bind_tables = @bind_tables.select{|r| r.end_with? '_table'}
      return unless @bind_tables.present?
      @bind_tables.each do |var|
        awesome_variables[var] ||= Blazer.data_sources[data_source].awesome_variables[var]
      end
      @bind_tables.each do |var|
        variable = awesome_variables[var]
        if variable.present? && variable['type'] == 'table'
          prefix_table = variable['value']['name']
          suffix = eval(variable['value']['suffix'])
          value =prefix_table + suffix
          statement.gsub!("{#{var}}", value )
        end
      end
    end

    def parse_smart_variables(var, data_source)
      smart_var_data_source =
          ([data_source] + Array(data_source.settings["inherit_smart_settings"]).map { |ds| Blazer.data_sources[ds] }).find { |ds| ds.smart_variables[var] }

      if smart_var_data_source
        query = smart_var_data_source.smart_variables[var]

        if query.is_a? Hash
          smart_var = query.map { |k,v| [v, k] }
        elsif query.is_a? Array
          smart_var = query.map { |v| [v, v] }
        elsif query
          result = smart_var_data_source.run_statement(query)
          smart_var = result.rows.map { |v| v.reverse }
          error = result.error if result.error
        end
      end

      [smart_var, error]
    end

    def parse_awesome_variables(var, data_source)
      # awesome_var_data_source =
      #     ([data_source] + Array(data_source.settings["inherit_smart_settings"]).map { |ds| Blazer.data_sources[ds] }).find { |ds| ds.smart_variables[var] }
      awesome_var_data_source =
          ([data_source] + Array(data_source.settings["inherit_smart_settings"]).map { |ds| Blazer.data_sources[ds] }).find { |ds| ds.awesome_variables[var] }  # 이 부분도 추후 수정
      if awesome_var_data_source
        query = awesome_var_data_source.awesome_variables[var]

        if query.is_a? Hash
          awesome_var = query
        elsif query
          result = awesome_var_data_source.run_statement(query)
          awesome_var = result.rows.map { |v| v.reverse }
          error = result.error if result.error
        end
      end

      [awesome_var, error]
    end

    def variable_params
      params.except(:controller, :action, :id, :host, :query, :dashboard, :query_id, :query_ids, :table_names, :authenticity_token, :utf8, :_method, :commit, :statement, :data_source, :name, :fork_query_id, :blazer, :run_id).permit!
    end
    helper_method :variable_params

    def blazer_user
      send(Blazer.user_method) if Blazer.user_method && respond_to?(Blazer.user_method)
    end
    helper_method :blazer_user

    def render_errors(resource)
      @errors = resource.errors
      action = resource.persisted? ? :edit : :new
      render action, status: :unprocessable_entity
    end

    # do not inherit from ApplicationController - #120
    def default_url_options
      {}
    end

    # TODO  나중에는 바라보는 data_source에 따라 클라우드 서비스를 생성하도록 한다.
    def load_service(service = CloudService.new('google'))
      @cloud ||= service.cloud
    end
  end
end
