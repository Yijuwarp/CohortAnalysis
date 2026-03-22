export function SortableContext({ children }) {
  return children
}

export function useSortable() {
  return {
    attributes: {},
    listeners: {},
    setNodeRef: () => {},
    transform: null,
    transition: undefined,
  }
}

export const verticalListSortingStrategy = () => null

export function arrayMove(array, from, to) {
  const copy = [...array]
  const [item] = copy.splice(from, 1)
  copy.splice(to, 0, item)
  return copy
}
