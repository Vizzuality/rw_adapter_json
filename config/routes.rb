Rails.application.routes.draw do
  scope module: :v1, constraints: APIVersion.new(version: 1, current: true) do
    post   'query/:id',    to: 'connectors#show'
    post   'datasets',     to: 'connectors#create'
    delete 'datasets/:id', to: 'connectors#destroy'
  end
end
