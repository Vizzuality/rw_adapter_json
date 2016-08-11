Rails.application.routes.draw do
  scope module: :v1, constraints: APIVersion.new(version: 1, current: true) do
    post   'query/:id',                  to: 'connectors#show'
    post   'fields/:id',                 to: 'connectors#fields'
    post   'datasets',                   to: 'connectors#create'
    post   'datasets/:id',               to: 'connectors#update'
    post   'datasets/:id/overwrite',     to: 'connectors#overwrite'
    post   'datasets/:id/data/:data_id', to: 'connectors#update_data'
    delete 'datasets/:id/data/:data_id', to: 'connectors#delete_data'
    delete 'datasets/:id',               to: 'connectors#destroy'
    get    'info',                       to: 'connectors#info'
  end
end
